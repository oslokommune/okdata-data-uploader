import os
import tempfile
from unittest.mock import patch

import awswrangler as wr
import boto3
import deltalake as dl
from moto import mock_aws
from pytest import fixture

from uploader.dataset import dataframe_from_dict


@fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmpdirname:
        yield tmpdirname


def write_deltalake(path, data, **kwargs):
    # https://github.com/aws/aws-sdk-pandas/blob/main/awswrangler/s3/_write_deltalake.py
    if "s3_allow_unsafe_rename" in kwargs:
        del kwargs["s3_allow_unsafe_rename"]

    df = dataframe_from_dict(data)
    schema = wr._data_types.pyarrow_schema_from_pandas(df=df, index=None)
    table = wr._arrow._df_to_table(df, schema)

    return dl.write_deltalake(
        table_or_uri=path,
        data=table,
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


@fixture
def mocked_wr_read_deltalake(temp_dir, existing_data):
    def side_effect(*args, **kwargs):
        if not existing_data:
            raise dl.exceptions.TableNotFoundError

        write_deltalake(temp_dir, existing_data)

        return read_deltalake(temp_dir)

    with patch.object(wr.s3, "read_deltalake", side_effect=side_effect):
        yield


@fixture
def dataset():
    return {"Id": "test-dataset", "accessRights": "public"}


@fixture
def dynamodb(dataset):
    """Create a mock DynamoDB table named `dataset-subscriptions`.

    Mimics the properties of the real `dataset-subscriptions`.
    """
    with mock_aws():
        dynamodb = boto3.resource("dynamodb", region_name=os.environ["AWS_REGION"])
        table = dynamodb.create_table(
            TableName="dataset-subscriptions",
            KeySchema=[{"AttributeName": "DatasetId", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "DatasetId", "AttributeType": "S"}],
            ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
        )
        table.put_item(
            Item={"DatasetId": dataset["Id"], "Subscribers": ["test@example.org"]}
        )
        yield dynamodb
