import json
import logging
import os

from aws_xray_sdk.core import patch_all, xray_recorder
from okdata.aws.logging import log_add, logging_wrapper
from okdata.aws.status import status_add, status_wrapper, TraceStatus

from uploader.common import generate_s3_path, get_and_validate_dataset, sdk_config
from uploader.dataset import handle_events

patch_all()

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))


@status_wrapper(sdk_config())
@logging_wrapper
@xray_recorder.capture("push_dataset_events")
def event_queue_handler(event, context):
    """Handle an event from the dataset event queue."""

    # Batch size on the trigger is 1, so there should never be more than one
    # record.
    record = event["Records"][0]

    status_add(trace_id=record["messageAttributes"]["trace_id"]["stringValue"])

    body = json.loads(record["body"])
    dataset_id = body["datasetId"]
    merge_on = body.get("mergeOn", [])
    version = body.get("version", "1")

    log_add(
        dataset_id=dataset_id,
        dataset_version=version,
        event_count=len(body["events"]),
    )

    dataset = get_and_validate_dataset(dataset_id, source_type="event")

    status_add(domain="dataset", domain_id=f"{dataset_id}/{version}")

    source_s3_path = generate_s3_path(
        dataset, f"{dataset_id}/{version}/latest", "processed", absolute=True
    )

    log_add(source_s3_path=source_s3_path)

    edition_id = handle_events(
        dataset, version, merge_on, source_s3_path, body["events"]
    )

    status_add(trace_status=TraceStatus.FINISHED)

    return {
        "statusCode": 200,
        "body": json.dumps({"editionId": edition_id}),
    }
