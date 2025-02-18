import pandas as pd
import pyarrow as pa
import pytest

from uploader.dataset import (
    append_to_dataset,
    dataframe_from_dict,
)
from uploader.errors import InvalidTypeError


@pytest.mark.parametrize(
    "data,schema",
    [
        # https://arrow.apache.org/docs/python/api/datatypes.html
        ([{"a": 2, "b": "bar", "c": None}], {"a": pa.int64(), "b": pa.string()}),
        (
            [
                {"a": 2, "b": "bar", "c": "baz"},
                {"a": 2, "b": "bar", "c": None},
            ],
            {"a": pa.int64(), "b": pa.string(), "c": pa.string()},
        ),
        ([{"a": 0}, {"a": 5000000000000}], {"a": pa.int64()}),
        ([{"a": 1}, {"a": 1.123}], {"a": pa.float64()}),
        ([{"a": True}, {"a": False}, {"a": None}], {"a": pa.bool_()}),
        # Incomplete dates should be strings.
        ([{"a": "2024"}], {"a": pa.string()}),
        ([{"a": "2024-10"}], {"a": pa.string()}),
        (
            [{"a": "2024-10-01"}, {"a": "1999-10-01"}],
            {"a": pa.date64()},
        ),
        (
            [{"a": "2024-10-01"}, {"a": "foo"}],
            {"a": pa.string()},
        ),
        ([{"a": "2024-10-22T14:43:47"}], {"a": pa.timestamp("us", tz="UTC")}),
        ([{"a": "2024-10-22T14:43:47Z"}], {"a": pa.timestamp("us", tz="UTC")}),
        ([{"a": "2024-10-22T14:43:47+02:00"}], {"a": pa.timestamp("us", tz="UTC")}),
        ([{"a": "2024-10-22T14:43:47.764186"}], {"a": pa.timestamp("us", tz="UTC")}),
        ([{"a": "2025-01-16T08:21:07.61978Z"}], {"a": pa.timestamp("us", tz="UTC")}),
        (
            [{"a": "2024-10-22T14:44:41.038797+02:00"}],
            {"a": pa.timestamp("us", tz="UTC")},
        ),
        # Mixed formats fallback to strings.
        (
            [
                {"a": "2024-10-22T14:43:47.764186"},
                {"a": "2024-10-22T14:44:41.038797+02:00"},
            ],
            {"a": pa.string()},
        ),
        (
            [{"a": "2024-10-22T14:43:47.764186"}, {"a": "2024-10-22"}],
            {"a": pa.string()},
        ),
    ],
)
def test_dataframe_from_dict(data, schema):
    df = dataframe_from_dict(data)

    for col in df.columns:
        dtype = df[col].dtype
        assert dtype == pd.ArrowDtype(schema[col])


@pytest.mark.parametrize(
    "existing_data,new_data",
    [
        ([{"a": 1}], [{"a": 2}]),
        ([{"a": 1, "b": "foo"}], [{"a": 2}]),
        ([{"a": 1, "b": "foo"}], [{"c": 2}]),
        ([], [{"a": 2, "b": "bar", "c": False}]),
        ([], [{"a": 2, "b": "bar", "c": None}]),
        ([], []),
        ([{"a": 1}], []),
    ],
)
def test_append_to_dataset(
    temp_dir,
    mocked_wr_read_deltalake,
    existing_data,
    new_data,
):
    target_df = pd.concat(
        [
            dataframe_from_dict(existing_data),
            dataframe_from_dict(new_data),
        ]
    ).reset_index(drop=True)

    merged_df = append_to_dataset("s3://foo/bar", new_data)

    assert merged_df.equals(target_df)


@pytest.mark.parametrize(
    "existing_data,new_data",
    [
        ([{"invalid_column": 1}], [{"invalid_column": "2"}]),
        ([{"invalid_column": "2024-10-22T14:43:31.012588"}], [{"invalid_column": "-"}]),
    ],
)
def test_append_to_dataset_mixed_types(
    mocked_wr_read_deltalake,
    existing_data,
    new_data,
):
    with pytest.raises(InvalidTypeError, match=r"invalid_column"):
        append_to_dataset("s3://foo/bar", new_data)
