import re
import tempfile
from datetime import datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

import awswrangler as wr
import deltalake as dl
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


def write_deltalake(path, data, **kwargs):
    # https://github.com/aws/aws-sdk-pandas/blob/main/awswrangler/s3/_write_deltalake.py
    if "s3_allow_unsafe_rename" in kwargs:
        del kwargs["s3_allow_unsafe_rename"]

    return dl.write_deltalake(
        table_or_uri=path,
        data=dataframe_from_dict(data),
        **kwargs,
    )


def read_deltalake(path):
    # https://github.com/aws/aws-sdk-pandas/blob/main/awswrangler/s3/_read_deltalake.py
    return (
        dl.DeltaTable(
            table_uri=path,
        )
        .to_pyarrow_table()
        .to_pandas(
            **wr._data_types.pyarrow2pandas_defaults(
                use_threads=False, kwargs={}, dtype_backend="pyarrow"
            )
        )
    )


@pytest.fixture(scope="function")
def temp_dir():
    with tempfile.TemporaryDirectory() as tmpdirname:
        yield tmpdirname


@pytest.fixture
def mocked_wr_read_deltalake(temp_dir, existing_data):
    def side_effect(*args, **kwargs):
        if not existing_data:
            raise dl.exceptions.TableNotFoundError

        write_deltalake(temp_dir, existing_data)

        return read_deltalake(temp_dir)

    with patch.object(wr.s3, "read_deltalake", side_effect=side_effect):
        yield


@pytest.fixture
def mocked_wr_to_deltalake(temp_dir):
    def side_effect(path, data, **kwargs):
        write_deltalake(temp_dir, data, **kwargs)

    with patch.object(wr.s3, "to_deltalake", side_effect=side_effect):
        yield


@pytest.mark.parametrize(
    "existing_data,new_data",
    [
        ([{"a": 1}], [{"a": 2}]),
        ([{"a": 1, "b": "foo"}], [{"a": 2}]),
        ([{"a": 1, "b": "foo"}], [{"c": 2}]),
        ([], [{"a": 2, "b": "bar", "c": False}]),
        ([], [{"a": 2, "b": "bar", "c": None}]),
    ],
)
def test_append_to_dataset(
    temp_dir,
    mocked_wr_read_deltalake,
    mocked_wr_to_deltalake,
    existing_data,
    new_data,
):
    append_to_dataset("s3://foo/bar", new_data)

    df = read_deltalake(temp_dir)
    df2 = pd.concat([dataframe_from_dict(existing_data), dataframe_from_dict(new_data)])
    df2 = df2.reset_index(drop=True)

    assert df.equals(df2)


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
