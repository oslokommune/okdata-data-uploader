import awswrangler as wr
import pandas as pd
import pyarrow as pa
from deltalake.exceptions import TableNotFoundError

from okdata.aws.logging import log_duration

from uploader.errors import InvalidTypeError, MissingMergeColumnsError


def add_to_dataset(s3_path, data, merge_on=[]):
    """Return the dataset found at `s3_path` with `data` added to it.

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
            merged_data = log_duration(
                lambda: events.combine_first(existing_dataset),
                "dataset_combine_first_duration",
            )
            # Turn the index back into ordinary columns
            merged_data.reset_index(inplace=True)
        else:
            merged_data = log_duration(
                lambda: pd.concat([existing_dataset, events]), "dataset_concat_duration"
            )
    except TableNotFoundError:
        merged_data = events

    # Ensure that we have no index
    merged_data = merged_data.reset_index(drop=True)

    # Ensure no columns contain mixed types
    mixed_columns = [c for c in merged_data if merged_data[c].dtype == "object"]

    if mixed_columns:
        raise InvalidTypeError(
            f"Invalid or mixed types detected in column(s): {', '.join(mixed_columns)}"
        )

    return merged_data


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
