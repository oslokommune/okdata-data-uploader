import json
import logging
import os

import awswrangler as wr
import boto3
import pandas as pd
import pyarrow as pa
from deltalake.exceptions import TableNotFoundError
from okdata.aws.logging import log_add, log_duration, log_exception
from okdata.sdk.data.dataset import Dataset

from uploader.alerts import alert_if_new_columns
from uploader.common import generate_s3_path, sdk_config
from uploader.errors import AlertEmailError, InvalidTypeError, MissingMergeColumnsError

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))


def handle_events(dataset, version, merge_on, source_s3_path, events):
    dataset_id = dataset["Id"]

    merged_data, new_columns = add_to_dataset(source_s3_path, events, merge_on)
    sdk = Dataset(sdk_config())
    edition = sdk.auto_create_edition(dataset_id, version)

    target_s3_path_processed = generate_s3_path(
        dataset, edition["Id"], "processed", absolute=True
    )
    target_s3_path_raw = generate_s3_path(dataset, edition["Id"], "raw")

    log_add(
        target_s3_path_processed=target_s3_path_processed,
        target_s3_path_raw=target_s3_path_raw,
    )

    # Write the raw input data
    s3 = boto3.client("s3", region_name=os.environ["AWS_REGION"])
    s3.put_object(
        Body=json.dumps(events),
        Bucket=os.environ["BUCKET"],
        Key=f"{target_s3_path_raw}/data.json",
    )

    # Clean out any existing data in `latest`
    log_duration(
        lambda: wr.s3.delete_objects(source_s3_path), "delete_objects_duration"
    )

    # Write merged data to both the new edition and to `latest`
    for i, path in enumerate([target_s3_path_processed, source_s3_path]):
        logger.info(f"Writing the merged data to {path}...")
        log_duration(
            lambda: wr.s3.to_deltalake(
                df=merged_data,
                path=path,
                mode="overwrite",
                schema_mode="merge",
                s3_allow_unsafe_rename=True,
            ),
            f"to_deltalake_{i}_duration",
        )
        logger.info("...done")

    # Create new distribution
    edition_id = edition["Id"]
    log_add(edition_id=edition_id)

    distribution = sdk.create_distribution(
        dataset_id,
        version,
        edition_id.split("/")[2],
        data={
            "distribution_type": "file",
            "content_type": "application/vnd.apache.parquet",
            "filenames": [
                obj.removeprefix(f"{target_s3_path_processed}/")
                for obj in wr.s3.list_objects(target_s3_path_processed)
            ],
        },
        retries=3,
    )

    log_add(distribution_id=distribution["Id"])

    try:
        alert_if_new_columns(dataset_id, new_columns)
    except AlertEmailError as e:
        log_exception(e)

    return edition["Id"]


def add_to_dataset(s3_path, data, merge_on=[]):
    """Return the dataset found at `s3_path` with `data` added to it.

    Also return a set of new columns (if any) that weren't present in the
    existing dataset as the second element of the returned tuple.

    `merge_on` is a list of column names to optionally merge ("full join" in
    the SQL world) the data on. New data overrides old data on conflicting
    rows.

    If `merge_on` is empty, the new data is simply appended to the existing
    dataset.
    """
    # Create DataFrame with new data
    events = dataframe_from_dict(data)

    # Load existing dataset contents to DataFrame and add new objects. If the
    # dataset is empty, new data is written directly.
    try:
        existing_dataset = log_duration(
            lambda: wr.s3.read_deltalake(s3_path, dtype_backend="pyarrow"),
            "read_deltalake_duration",
        )

        if merge_on:
            try:
                events.set_index(merge_on, inplace=True)
                existing_dataset.set_index(merge_on, inplace=True)
            except KeyError as e:
                raise MissingMergeColumnsError(f"Missing ID column(s): {e}")
            # A note on efficiency: Local tests suggest that this should scale
            # well to at least tens of millions of rows.
            try:
                merged_data = log_duration(
                    lambda: events.combine_first(existing_dataset),
                    "dataset_combine_first_duration",
                )
            except ValueError:
                raise InvalidTypeError("Mixed types detected")
            # Turn the index back into ordinary columns
            merged_data.reset_index(inplace=True)
        else:
            merged_data = log_duration(
                lambda: pd.concat([existing_dataset, events]), "dataset_concat_duration"
            )
    except TableNotFoundError:
        existing_dataset = None
        merged_data = events

    # Ensure that we have no index
    merged_data = merged_data.reset_index(drop=True)

    # Ensure no columns contain mixed types
    mixed_columns = [c for c in merged_data if merged_data[c].dtype == "object"]

    if mixed_columns:
        raise InvalidTypeError(
            f"Invalid or mixed types detected in column(s): {', '.join(mixed_columns)}"
        )

    # Detect new columns
    new_columns = (
        set()
        if existing_dataset is None
        else set(merged_data.columns) - set(existing_dataset.columns)
    )

    return merged_data, new_columns


def dataframe_from_dict(data):
    # Construct DataFrame from `data`. Drop empty columns and convert
    # columns to the best possible dtypes using pyarrow.
    df = pd.DataFrame.from_dict(data)
    df = df.dropna(how="all", axis="columns")
    df = df.convert_dtypes(dtype_backend="pyarrow")
    df = df.apply(_infer_column_dtype_from_input)
    return df


def _infer_column_dtype_from_input(col):
    recognized_datetime_formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%S.%f%z",
    ]

    # Detect columns containing date(time) values and attempt casting to relevant
    # dtype. If it fails, keep existing string type.
    if getattr(col.dtypes, "pyarrow_dtype", None) == "string":
        try:
            series = pd.to_datetime(col, format="%Y-%m-%d")
            return series.astype(pd.ArrowDtype(pa.date64()))
        except ValueError:
            pass

        for dt_format in recognized_datetime_formats:
            try:
                series = pd.to_datetime(col, format=dt_format, utc=True)
                return series.astype(pd.ArrowDtype(pa.timestamp("us", tz="UTC")))
            except ValueError:
                pass

    return col
