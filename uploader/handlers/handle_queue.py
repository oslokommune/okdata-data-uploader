import json
import logging
import os

from aws_xray_sdk.core import patch_all, xray_recorder
from okdata.aws.logging import log_add, logging_wrapper

from uploader.common import generate_s3_path, get_and_validate_dataset
from uploader.dataset import handle_events

patch_all()

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))


@logging_wrapper
@xray_recorder.capture("push_dataset_events")
def handler(event, context):
    """Handle an event from the dataset event queue.

    TODO: Missing error reporting back to the user in cases where handling of
    an event fails.
    """
    # Batch size on the trigger is 1, so there should never be more than one
    # record.
    body = json.loads(event["Records"][0]["body"])
    dataset_id = body["datasetId"]
    merge_on = body.get("mergeOn", [])
    version = body.get("version", "1")

    log_add(
        dataset_id=dataset_id,
        dataset_version=version,
        event_count=len(body["events"]),
    )

    dataset = get_and_validate_dataset(dataset_id, source_type="event")

    source_s3_path = generate_s3_path(
        dataset, f"{dataset_id}/{version}/latest", "processed", absolute=True
    )

    log_add(source_s3_path=source_s3_path)

    edition_id = handle_events(
        dataset, version, merge_on, source_s3_path, body["events"]
    )

    return {
        "statusCode": 200,
        "body": json.dumps({"editionId": edition_id}),
    }
