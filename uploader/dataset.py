import awswrangler as wr
import pandas as pd
import pyarrow as pa
from deltalake.exceptions import TableNotFoundError

from uploader.errors import MixedTypeError


def append_to_dataset(s3_path, data):
    # Create DataFrame with new data
    events = dataframe_from_dict(data)

    # Load existing dataset contents to DataFrame and concatenate new objects. If
    # the dataset is empty, new data is written directly.
    try:
        existing_dataset = wr.s3.read_deltalake(
            s3_path,
            dtype_backend="pyarrow",
        )
        merged_data = pd.concat([existing_dataset, events])
    except TableNotFoundError:
        merged_data = events

    # Ensure that we have no index
    merged_data = merged_data.reset_index(drop=True)

    # Ensure no columns contains mixed types
    mixed_columns = [c for c in merged_data if merged_data[c].dtype == "object"]

    if mixed_columns:
        raise MixedTypeError(
            f"Mixed types detected in column(s): {', '.join(mixed_columns)}"
        )

    # Write raw events to new edition `raw`
    # create_raw_edition(data)

    # Write data to new edition and `latest` in `processed`

    # `okdata-pipeline`
    # def write_data_to_latest(s3_sources, output_prefix):
    #     output_prefix_latest = re.sub("edition=.*/", "latest/", output_prefix)
    #     s3_service.delete_from_prefix(output_prefix_latest)
    #     copy_data(s3_sources, output_prefix_latest)

    wr.s3.to_deltalake(
        s3_path,
        merged_data,
        mode="overwrite",
        schema_mode="merge",
        s3_allow_unsafe_rename=True,
    )


def dataframe_from_dict(data):
    # Construct DataFrame from `data`. Drop empty columns and convert
    # columns to the best possible dtypes using pyarrow.
    df = pd.DataFrame.from_dict(data)
    df = df.dropna(how="all", axis="columns")
    df = df.convert_dtypes(dtype_backend="pyarrow")
    df = df.apply(_infer_column_dtype_from_input)
    return df


def _infer_column_dtype_from_input(col):
    # Detect columns containing date(time) values and attempt casting to relevant
    # dtype. If it fails, keep existing string type.
    if getattr(col.dtypes, "pyarrow_dtype", None) == "string":
        try:
            series = pd.to_datetime(col, format="ISO8601", utc=True)
            return series.astype(pd.ArrowDtype(pa.timestamp("us", tz="UTC")))
        except ValueError:
            pass

    return col
