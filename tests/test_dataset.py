import re
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import pytest

from uploader.dataset import (
    append_to_dataset,
    dataframe_from_dict,
)
from uploader.errors import MixedTypeError


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
        (
            [
                {"a": datetime.now().isoformat()},
                {
                    "a": datetime.now()
                    .replace(tzinfo=ZoneInfo("Europe/Oslo"))
                    .isoformat()
                },
            ],
            {"a": pa.timestamp("us", tz="UTC")},
        ),
        (
            [{"a": "2024-10-01"}, {"a": "1999-10-01"}],
            {"a": pa.timestamp("us", tz="UTC")},
        ),
        (
            [{"a": "2024-10-01"}, {"a": "foo"}],
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
        ([{"a": 1}], [{"a": "2"}]),
        ([{"a": datetime.now().isoformat()}], [{"a": "-"}]),
    ],
)
def test_append_to_dataset_mixed_types(
    mocked_wr_read_deltalake,
    existing_data,
    new_data,
):
    with pytest.raises(
        MixedTypeError,
        match=re.escape("Mixed types detected in column(s): a"),
    ):
        append_to_dataset("s3://foo/bar", new_data)
