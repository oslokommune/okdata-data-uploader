import concurrent.futures
import json
from unittest.mock import patch

import boto3
from moto import mock_aws

from uploader.handlers.push_dataset_events import handler


def _mock_event(body):
    """Return a mock AWS Lambda event payload."""
    return {"body": json.dumps(body), "headers": {"Authorization": ""}}


def _mock_dynamodb():
    """Create a mock DynamoDB table named `delta-write-lock`.

    Mimics the properties of the real `delta-write-lock`.
    """
    dynamodb = boto3.resource("dynamodb", region_name="eu-west-1")
    dynamodb.create_table(
        TableName="delta-write-lock",
        KeySchema=[{"AttributeName": "DatasetId", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "DatasetId", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
    )
    return dynamodb


def test_handler_missing_dataset_id():
    res = handler(_mock_event(None), None)
    assert res["statusCode"] == 400


def test_handler_missing_events():
    res = handler(_mock_event({"datasetId": "foo"}), None)
    assert res["statusCode"] == 400


def test_handler_no_events():
    res = handler(_mock_event({"datasetId": "foo", "events": []}), None)
    assert res["statusCode"] == 400


def test_handler_unauthorized():
    res = handler(_mock_event({"datasetId": "foo", "events": [{"a": 1}]}), None)
    assert res["statusCode"] == 403


@mock_aws
@patch("uploader.handlers.push_dataset_events.resource_authorizer.has_access")
@patch("uploader.handlers.push_dataset_events.get_and_validate_dataset")
@patch("uploader.handlers.push_dataset_events.LOCK_WAIT_SECONDS", 1)  # quicker tests
@patch("uploader.handlers.push_dataset_events.LOCK_RETRIES", 3)  # quicker tests
def test_handler_dataset_locked(get_and_validate_dataset, has_access):
    has_access.return_value = True
    get_and_validate_dataset.return_value = {"Id": "foo", "accessRights": "non-public"}
    dynamodb = _mock_dynamodb()

    lock_table = dynamodb.Table("delta-write-lock")
    lock_table.put_item(Item={"DatasetId": "foo"})

    res = handler(_mock_event({"datasetId": "foo", "events": [{"a": 1}]}), None)
    assert res["statusCode"] == 409


@mock_aws
@patch("uploader.handlers.push_dataset_events._handle_events")
@patch("uploader.handlers.push_dataset_events.resource_authorizer.has_access")
@patch("uploader.handlers.push_dataset_events.get_and_validate_dataset")
def test_handler_single_valid_event(
    get_and_validate_dataset, has_access, handle_events
):
    has_access.return_value = True
    get_and_validate_dataset.return_value = {"Id": "foo", "accessRights": "non-public"}
    handle_events.return_value = "new-edition"
    _mock_dynamodb()

    res = handler(_mock_event({"datasetId": "foo", "events": [{"a": 1}]}), None)
    assert res["statusCode"] == 201


@mock_aws
@patch("uploader.handlers.push_dataset_events._handle_events")
@patch("uploader.handlers.push_dataset_events.resource_authorizer.has_access")
@patch("uploader.handlers.push_dataset_events.get_and_validate_dataset")
def test_handler_multipe_valid_events_in_parallel(
    get_and_validate_dataset, has_access, handle_events
):
    has_access.return_value = True
    get_and_validate_dataset.return_value = {"Id": "foo", "accessRights": "non-public"}
    handle_events.return_value = "new-edition"
    _mock_dynamodb()

    def run_handler(i):
        # Unwrap the handler from `logging_wrapper` since `logging_wrapper`
        # doesn't support asynchronous executions.
        return handler.__wrapped__(
            _mock_event({"datasetId": "foo", "events": [{"a": i}]}), None
        )

    with concurrent.futures.ThreadPoolExecutor() as ex:
        # Simulate 20 concurrent API calls.
        futures = [ex.submit(run_handler, i) for i in range(0, 20)]

    assert all(f.result()["statusCode"] == 201 for f in futures)
    assert handle_events.call_count == 20
