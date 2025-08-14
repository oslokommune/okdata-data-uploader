import os
from unittest.mock import patch

import boto3
from moto import mock_aws

from uploader.alerts import alert_if_new_columns


def _mock_dynamodb():
    """Create a mock DynamoDB table named `dataset-subscriptions`.

    Mimics the properties of the real `dataset-subscriptions`.
    """
    dynamodb = boto3.resource("dynamodb", region_name=os.environ["AWS_REGION"])
    dynamodb.create_table(
        TableName="dataset-subscriptions",
        KeySchema=[{"AttributeName": "DatasetId", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "DatasetId", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
    )
    return dynamodb


@mock_aws
@patch("uploader.alerts._send_email")
def test_alert_if_new_columns_no_new_columns(send_email):
    dynamodb = _mock_dynamodb()

    table = dynamodb.Table("dataset-subscriptions")
    table.put_item(
        Item={"DatasetId": "test-dataset", "Subscribers": ["test@example.org"]}
    )

    alert_if_new_columns("test-dataset", set())

    send_email.assert_not_called()


@mock_aws
@patch("uploader.alerts._send_email")
def test_alert_if_new_columns_no_subscribers(send_email):
    _mock_dynamodb()

    alert_if_new_columns("test-dataset", {"new_column"})

    send_email.assert_not_called()


@mock_aws
@patch("uploader.alerts._send_email")
def test_alert_if_new_columns_single_new_column(send_email):
    dynamodb = _mock_dynamodb()

    table = dynamodb.Table("dataset-subscriptions")
    table.put_item(
        Item={"DatasetId": "test-dataset", "Subscribers": ["test@example.org"]}
    )

    alert_if_new_columns("test-dataset", {"new_column"})

    send_email.assert_called_once_with(
        ["test@example.org"],
        "En ny kolonne har blitt lagt til datasettet 'test-dataset':\n- new_column",
    )


@mock_aws
@patch("uploader.alerts._send_email")
def test_alert_if_new_columns_multiple_new_columns(send_email):
    dynamodb = _mock_dynamodb()

    table = dynamodb.Table("dataset-subscriptions")
    table.put_item(
        Item={"DatasetId": "test-dataset", "Subscribers": ["test@example.org"]}
    )

    alert_if_new_columns("test-dataset", {"b_col", "a_col"})

    send_email.assert_called_once_with(
        ["test@example.org"],
        "Nye kolonner har blitt lagt til datasettet 'test-dataset':\n- a_col\n- b_col",
    )
