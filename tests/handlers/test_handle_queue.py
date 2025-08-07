import json
from unittest.mock import patch

import pytest

from uploader.handlers.handle_queue import handler


@pytest.fixture
def mock_event():
    return {
        "Records": [
            {
                "messageId": "9e10fa0f-fa5d-16a5-099b-c82b304df1f8",
                "receiptHandle": "abc123",
                "body": '{"datasetId": "test-dataset", "mergeOn": ["id"], "events": [{"id": 1, "value": 5}]}',
                "attributes": {
                    "ApproximateReceiveCount": "1",
                    "AWSTraceHeader": "Root=1-b5a9daed-357f662c2c285cf280b61d38;Parent=6654c0335342e1d2;Sampled=1;Lineage=1:fe460cdd:0",
                    "SentTimestamp": "1752133103084",
                    "SequenceNumber": "18895290148099055616",
                    "MessageGroupId": "data-uploader-test-dataset",
                    "SenderId": "AROA6MKHNZ5M2JTSYU4F3:data-uploader-dev-push-dataset-events-to-queue",
                    "MessageDeduplicationId": "a643f3071394db014cba82ca1a4e68c69e249e885f9d0bf63d051b877bd94ed5",
                    "ApproximateFirstReceiveTimestamp": "1752133103084",
                },
                "messageAttributes": {},
                "md5OfBody": "2a28ab2ae56e1c0ba54bddeb3ece787e",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:eu-west-1:123456789101:DatasetEvents.fifo",
                "awsRegion": "eu-west-1",
            }
        ]
    }


@patch("uploader.handlers.handle_queue.get_and_validate_dataset")
@patch("uploader.handlers.handle_queue.handle_events")
def test_handler(handle_events, get_and_validate_dataset, mock_event):
    get_and_validate_dataset.return_value = {
        "Id": "test-dataset",
        "accessRights": "non-public",
    }
    handle_events.return_value = "new-edition"

    res = handler(mock_event, None)

    handle_events.assert_called_once_with(
        {"Id": "test-dataset", "accessRights": "non-public"},
        "1",
        ["id"],
        "s3://testbucket/processed/red/test-dataset/version=1/latest",
        [{"id": 1, "value": 5}],
    )

    assert res["statusCode"] == 200
    assert json.loads(res["body"])["editionId"] == "new-edition"
