import json
import logging
import os
import time
from datetime import datetime, timezone
from json.decoder import JSONDecodeError

import boto3
from aws_xray_sdk.core import patch_all, xray_recorder
from botocore.exceptions import ClientError
from jsonschema import validate, ValidationError, SchemaError
from okdata.aws.logging import log_add, log_exception, logging_wrapper
from okdata.resource_auth import ResourceAuthorizer

from uploader.common import error_response, generate_s3_path, get_and_validate_dataset
from uploader.dataset import handle_events
from uploader.errors import (
    DatasetNotFoundError,
    InvalidSourceTypeError,
    InvalidTypeError,
    MissingMergeColumnsError,
)
from uploader.schema import get_model_schema

patch_all()

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))

LOCK_WAIT_SECONDS = 5
LOCK_RETRIES = 5

resource_authorizer = ResourceAuthorizer()


def _handler_v1(dataset, version, merge_on, events):
    """Synchronous event handler.

    To be phased out in favor of the implementation `_handler_v2` which is
    based on asynchronous handling in SQS.
    """
    dataset_id = dataset["Id"]

    source_s3_path = generate_s3_path(
        dataset, f"{dataset_id}/{version}/latest", "processed", absolute=True
    )

    log_add(source_s3_path=source_s3_path)

    dynamodb = boto3.resource("dynamodb", region_name=os.environ["AWS_REGION"])
    lock_table = dynamodb.Table("delta-write-lock")
    locked = False
    tries = 0
    edition_id = None

    while not edition_id and tries < LOCK_RETRIES:
        try:
            logger.info(f"Locking dataset {dataset_id}...")
            lock_table.put_item(
                Item={
                    "DatasetId": dataset_id,
                    "Timestamp": datetime.now(timezone.utc).isoformat(),
                },
                ConditionExpression="attribute_not_exists(DatasetId)",
            )
            logger.info("...done")
            locked = True
            edition_id = handle_events(
                dataset, version, merge_on, source_s3_path, events
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                logger.info(
                    f"Dataset was locked; waiting for {LOCK_WAIT_SECONDS} seconds..."
                )
                time.sleep(LOCK_WAIT_SECONDS)
                tries += 1
                logger.info(f"...done, trying again (attempt #{tries + 1})")
            else:
                raise
        except InvalidTypeError as e:
            log_add(exc_info=e)
            return error_response(400, str(e))
        except MissingMergeColumnsError as e:
            log_add(exc_info=e)
            return error_response(422, str(e))
        finally:
            if locked:
                logger.info(f"Unlocking dataset {dataset_id}...")
                lock_table.delete_item(Key={"DatasetId": dataset_id})
                logger.info("...done")

    if not edition_id:
        log_exception(
            f"The dataset '{dataset_id}' remained write-locked after several retries"
        )
        return error_response(
            409,
            "The dataset remains write-locked after several retries. This "
            "should not happen, please contact Dataspeilet.",
        )

    return {
        "statusCode": 201,
        "body": json.dumps({"editionId": edition_id}),
    }


def _handler_v2(event, dataset_id):
    """Alternate handler based on SQS.

    To become the default in favor of `_handler_v1` which does synchronous
    message handling.
    """
    if len(event["body"].encode("utf-8", "ignore")) >= 262144:  # (256 KiB)
        return error_response(400, "Body is too large; must be below 256 KiB")

    sqs = boto3.resource("sqs", region_name=os.environ["AWS_REGION"])

    try:
        queue = sqs.get_queue_by_name(QueueName=os.environ["EVENT_QUEUE_NAME"])
        queue.send_message(
            MessageGroupId=f"data-uploader-{dataset_id}",
            MessageBody=event["body"],
        )
    except ClientError as e:
        log_add(exc_info=e)
        error_response(
            503,
            "Couldn't push data to the queue. Please try again, or contact "
            "Dataspeilet if the problem persists.",
        )

    return {"statusCode": 200}


@logging_wrapper
@xray_recorder.capture("push_dataset_events")
def handler(event, context):
    try:
        body = json.loads(event["body"])
        validate(body, get_model_schema("pushEventsRequest"))
        dataset_id = body["datasetId"]
        merge_on = body.get("mergeOn", [])
        version = body.get("version", "1")

        log_add(
            dataset_id=dataset_id,
            merge_on=merge_on,
            dataset_version=version,
            event_count=len(body["events"]),
        )
    except (JSONDecodeError, TypeError) as e:
        log_add(exc_info=e)
        return error_response(400, "Body is not a valid JSON document")
    except ValidationError as e:
        log_add(exc_info=e)
        return error_response(
            400, f"JSON document does not conform to the given schema: {e.message}"
        )
    except (SchemaError, Exception) as e:
        log_add(exc_info=e)
        return error_response(500, "Internal server error")

    token = event["headers"]["Authorization"].split(" ")[-1]

    has_access = resource_authorizer.has_access(
        token, "okdata:dataset:write", f"okdata:dataset:{dataset_id}"
    )
    log_add(has_access=has_access)

    if not has_access:
        return error_response(403, "Forbidden")

    try:
        dataset = get_and_validate_dataset(dataset_id, source_type="event")
    except InvalidSourceTypeError as e:
        return error_response(400, str(e))
    except DatasetNotFoundError:
        return error_response(404, f"Dataset {dataset_id} does not exist")
    except Exception as e:
        log_add(exc_info=e)
        return error_response(500, "Internal server error")

    if body.get("apiVersion") == 2:
        return _handler_v2(event, dataset_id)
    return _handler_v1(dataset, version, merge_on, body["events"])
