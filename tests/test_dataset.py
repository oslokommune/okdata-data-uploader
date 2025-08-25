import os
from unittest.mock import Mock, patch

import boto3
import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from moto import mock_aws

from uploader.dataset import add_to_dataset, dataframe_from_dict, handle_events
from uploader.errors import InvalidTypeError, MissingMergeColumnsError


def _mock_s3():
    s3 = boto3.resource("s3", region_name=os.environ["AWS_REGION"])
    s3.create_bucket(
        Bucket=os.environ["BUCKET"],
        CreateBucketConfiguration={"LocationConstraint": os.environ["AWS_REGION"]},
    )


@mock_aws
@patch("uploader.dataset.add_to_dataset")
@patch("uploader.dataset.Dataset")
@patch("uploader.dataset.wr.s3.to_deltalake")
@patch("uploader.dataset.alert_if_new_columns")
def test_handle_events_alert_if_new_columns(
    alert_if_new_columns, to_deltalake, Dataset, add_to_dataset, dataset
):
    _mock_s3()

    sdk = Mock()
    sdk.auto_create_edition.return_value = {"Id": f"{dataset['Id']}/1/new-edition"}
    sdk.create_distribution.return_value = {
        "Id": f"{dataset['Id']}/1/new-edition/bec60adb3f560543"
    }
    Dataset.return_value = sdk

    add_to_dataset.return_value = pd.DataFrame.from_dict(
        [{"id": 1, "new_col": 2}]
    ), set("new_col")

    handle_events(
        dataset,
        "1",
        [],
        f"s3://{os.environ['BUCKET']}/{dataset['Id']}/1/old-edition",
        {"id": 1, "data": 2},
    )

    alert_if_new_columns.assert_called_once_with("test-dataset", set("new_col"))


@mock_aws
@patch("uploader.alerts.get_secret")
@patch("uploader.dataset.add_to_dataset")
@patch("uploader.dataset.Dataset")
@patch("uploader.dataset.wr.s3.to_deltalake")
def test_handle_events_email_error(
    to_deltalake, Dataset, add_to_dataset, get_secret, requests_mock, dynamodb, dataset
):
    _mock_s3()

    edition_id = f"{dataset['Id']}/1/new-edition"

    sdk = Mock()
    sdk.auto_create_edition.return_value = {"Id": edition_id}
    sdk.create_distribution.return_value = {
        "Id": f"{dataset['Id']}/1/new-edition/bec60adb3f560543"
    }
    Dataset.return_value = sdk

    add_to_dataset.return_value = pd.DataFrame.from_dict(
        [{"id": 1, "new_col": 2}]
    ), set("new_col")

    get_secret.return_value = "mega-secret"

    requests_mock.register_uri("POST", os.environ["EMAIL_API_URL"], status_code=500)

    # Most importantly test that the call doesn't raise an exception even if
    # the HTTP call to the email API returned a 500 error response.
    assert (
        handle_events(
            dataset,
            "1",
            [],
            f"s3://{os.environ['BUCKET']}/{dataset['Id']}/1/old-edition",
            {"id": 1, "data": 2},
        )
        == edition_id
    )


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
def test_add_to_dataset(
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

    merged_df, _ = add_to_dataset("s3://foo/bar", new_data)

    assert merged_df.equals(target_df)


@pytest.mark.parametrize(
    "existing_data,new_data,expected_result",
    [
        (
            [{"id": 1, "data": 1}],
            [{"id": 2, "data": 2}],
            [{"id": 1, "data": 1}, {"id": 2, "data": 2}],
        ),
        (
            [{"id": 1}],
            [{"id": 2, "data": 1}],
            [{"id": 1, "data": np.nan}, {"id": 2, "data": 1}],
        ),
        (
            [{"id": 1, "data": 1}],
            [{"id": 2}],
            [{"id": 1, "data": 1}, {"id": 2, "data": np.nan}],
        ),
        (
            [{"id": 1, "data": "override-me"}, {"id": 2, "data": "keep-me"}],
            [{"id": 1, "data": "overridden"}, {"id": 3, "data": "foo"}],
            [
                {"id": 1, "data": "overridden"},
                {"id": 2, "data": "keep-me"},
                {"id": 3, "data": "foo"},
            ],
        ),
        (
            [{"id": 1, "data": 1}, {"id": 1, "data": 2}],
            [{"id": 1, "data": 5}, {"id": 3, "data": 3}],
            # Results in duplicates
            [{"id": 1, "data": 5}, {"id": 1, "data": 5}, {"id": 3, "data": 3}],
        ),
    ],
)
def test_merge_on_single_column(
    temp_dir,
    mocked_wr_read_deltalake,
    existing_data,
    new_data,
    expected_result,
):
    merged_df, _ = add_to_dataset("s3://foo/bar", new_data, ["id"])

    pd.testing.assert_frame_equal(
        merged_df,
        pd.DataFrame.from_dict(expected_result).reset_index(drop=True),
        check_dtype=False,
    )


@pytest.mark.parametrize(
    "existing_data,new_data",
    [
        ([{"data": 1}], [{"id": 1, "data": 2}]),
        ([{"id": 1, "data": 1}], [{"data": 2}]),
        ([{"data": 1}], [{"data": 2}]),
        ([{"id": 1, "data": 1}], [{"id": None, "data": 2}]),
        ([{"id": None, "data": 1}], [{"id": 1, "data": 2}]),
        ([{"id": None, "data": 1}], [{"id": None, "data": 2}]),
    ],
)
def test_merge_with_missing_merge_column(
    temp_dir, mocked_wr_read_deltalake, existing_data, new_data
):
    with pytest.raises(MissingMergeColumnsError):
        add_to_dataset("s3://foo/bar", new_data, ["id"])


@pytest.mark.parametrize(
    "existing_data,new_data,expected_result",
    [
        (
            [
                {"id1": 0, "id2": 0, "data": "zero", "data2": "zero"},
                {"id1": 1, "id2": 1, "data": "foo", "data2": "keep-me"},
            ],
            [
                {"id1": 1, "id2": 1, "data": "bar"},
                {"id1": 1, "id2": 2, "data": "bax"},
            ],
            [
                {"id1": 0, "id2": 0, "data": "zero", "data2": "zero"},
                {"id1": 1, "id2": 1, "data": "bar", "data2": "keep-me"},
                {"id1": 1, "id2": 2, "data": "bax", "data2": pd.NA},
            ],
        )
    ],
)
def test_merge_on_multiple_column(
    temp_dir,
    mocked_wr_read_deltalake,
    existing_data,
    new_data,
    expected_result,
):
    merged_df, _ = add_to_dataset("s3://foo/bar", new_data, ["id1", "id2"])

    pd.testing.assert_frame_equal(
        merged_df,
        pd.DataFrame.from_dict(expected_result).reset_index(drop=True),
        check_dtype=False,
    )


@pytest.mark.parametrize(
    "existing_data,new_data",
    [
        (
            [{"id": 1, "value": 1}],
            [{"id": "invalid", "value": 2}],
        ),
        (
            [{"id": 1, "value": 1}, {"id": 1, "value": 1}],
            [{"id": "invalid", "value": 2}],
        ),
    ],
)
def test_merge_on_invalid_type(
    temp_dir, mocked_wr_read_deltalake, existing_data, new_data
):
    with pytest.raises(InvalidTypeError):
        add_to_dataset("s3://foo/bar", new_data, ["id"])


@pytest.mark.parametrize(
    "existing_data,new_data",
    [
        ([{"invalid_column": 1}], [{"invalid_column": "2"}]),
        ([{"invalid_column": "2024-10-22T14:43:31.012588"}], [{"invalid_column": "-"}]),
    ],
)
def test_add_to_dataset_mixed_types(
    mocked_wr_read_deltalake,
    existing_data,
    new_data,
):
    with pytest.raises(InvalidTypeError, match=r"invalid_column"):
        add_to_dataset("s3://foo/bar", new_data)


@pytest.mark.parametrize(
    "existing_data,new_data,expected_new_columns",
    [
        ([{"a": 1}], [{"b": 2}], {"b"}),
        ([{"a": 1}], [{"a": 2}, {"b": 3}], {"b"}),
        ([{"a": 1}, {"b": 2}], [{"b": 3}, {"c": 4}], {"c"}),
        ([{"a": 1}, {"b": 2}], [{"c": 3}, {"d": 4}], {"c", "d"}),
    ],
)
def test_add_to_dataset_new_columns(
    temp_dir, mocked_wr_read_deltalake, existing_data, new_data, expected_new_columns
):
    _, new_columns = add_to_dataset("s3://foo/bar", new_data)
    assert new_columns == expected_new_columns


@pytest.mark.parametrize(
    "existing_data,new_data",
    [
        ([], [{"a": 1}]),
        ([{"a": 1}], [{"a": 2}]),
        ([{"a": 1}, {"b": 2}], [{"a": 3}]),
        ([{"a": 1}, {"b": 2}], [{"a": 3}, {"b": 4}]),
        ([{"a": 1}, {"b": 2}], [{"b": 3}]),
    ],
)
def test_add_to_dataset_no_new_columns(
    temp_dir, mocked_wr_read_deltalake, existing_data, new_data
):
    _, new_columns = add_to_dataset("s3://foo/bar", new_data)
    assert new_columns == set()


@pytest.mark.parametrize(
    "existing_data,new_data",
    [
        ([{"id": 1, "a": 1}], [{"id": 1, "a": 2}]),
        ([{"id": 1, "a": 1}], [{"id": 2, "a": 2}]),
    ],
)
def test_add_to_dataset_with_merge_no_new_columns(
    temp_dir, mocked_wr_read_deltalake, existing_data, new_data
):
    _, new_columns = add_to_dataset("s3://foo/bar", new_data, ["id"])
    assert new_columns == set()
