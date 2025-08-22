import os
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

from uploader.alerts import alert_if_new_columns


@pytest.fixture
def test_dataset():
    return {"DatasetId": "test-dataset", "Subscribers": ["test@example.org"]}


@pytest.fixture
def dynamodb(test_dataset):
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
        table.put_item(Item=test_dataset)
        yield dynamodb


@patch("uploader.alerts._send_email")
def test_alert_if_new_columns_no_new_columns(send_email, test_dataset, dynamodb):
    alert_if_new_columns(test_dataset["DatasetId"], set())

    send_email.assert_not_called()


@patch("uploader.alerts._send_email")
def test_alert_if_new_columns_no_subscribers(send_email, test_dataset, dynamodb):
    table = dynamodb.Table("dataset-subscriptions")
    table.delete_item(Key={"DatasetId": test_dataset["DatasetId"]})

    alert_if_new_columns(test_dataset["DatasetId"], {"new_column"})

    send_email.assert_not_called()


@patch("uploader.alerts._send_email")
def test_alert_if_new_columns_single_new_column(send_email, test_dataset, dynamodb):
    alert_if_new_columns(test_dataset["DatasetId"], {"new_column"})

    send_email.assert_called_once_with(
        ["test@example.org"],
        "En ny kolonne har blitt lagt til datasettet 'test-dataset':\n- new_column",
    )


@patch("uploader.alerts._send_email")
def test_alert_if_new_columns_multiple_new_columns(send_email, test_dataset, dynamodb):
    alert_if_new_columns(test_dataset["DatasetId"], {"b_col", "a_col"})

    send_email.assert_called_once_with(
        ["test@example.org"],
        "Nye kolonner har blitt lagt til datasettet 'test-dataset':\n- a_col\n- b_col",
    )
