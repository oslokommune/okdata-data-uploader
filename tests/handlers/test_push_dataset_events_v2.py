"""Tests for version 2 of the dataset events API.

Can be merged into `test_push_dataset_events` once version 2 becomes the
default.
"""

import json
import os
from unittest.mock import patch

import boto3
from moto import mock_aws

from uploader.handlers.push_dataset_events import handler


def _mock_sqs():
    """Create a mock SQS queue.

    Mimics the properties of the real queue.
    """
    sqs = boto3.resource("sqs", region_name=os.environ["AWS_REGION"])
    sqs.create_queue(
        QueueName=os.environ["EVENT_QUEUE_NAME"],
        Attributes={
            "FifoQueue": "true",
            "ContentBasedDeduplication": "true",
        },
    )
    return sqs


def _mock_event(body):
    """Return a mock AWS Lambda event payload."""
    if body:
        # TODO: Can be removed once API version 2 is the default
        body["apiVersion"] = 2
    return {
        "body": json.dumps(body),
        "headers": {"Authorization": ""},
        "requestContext": {"authorizer": {"principalId": "test"}},
    }


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
@patch("uploader.handlers.push_dataset_events.create_status_trace")
def test_handler_single_valid_event(
    create_status_trace, get_and_validate_dataset, has_access
):
    has_access.return_value = True
    get_and_validate_dataset.return_value = {"Id": "foo", "accessRights": "non-public"}
    create_status_trace.return_value = {"trace_id": "abc-123"}
    _mock_sqs()

    res = handler(_mock_event({"datasetId": "foo", "events": [{"a": 1}]}), None)
    assert res["statusCode"] == 200
    assert json.loads(res["body"])["trace_id"] == "abc-123"


@patch("uploader.handlers.push_dataset_events.resource_authorizer.has_access")
@patch("uploader.handlers.push_dataset_events.get_and_validate_dataset")
def test_handler_event_too_large(get_and_validate_dataset, has_access):
    has_access.return_value = True
    get_and_validate_dataset.return_value = {"Id": "foo", "accessRights": "non-public"}

    res = handler(
        _mock_event({"datasetId": "foo", "events": [{"a": "x" * (256 * 2**10)}]}), None
    )
    assert res["statusCode"] == 400
