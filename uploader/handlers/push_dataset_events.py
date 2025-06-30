import json
import os
import time
from datetime import datetime, timezone
from json.decoder import JSONDecodeError

import awswrangler as wr
import boto3
from aws_xray_sdk.core import patch_all, xray_recorder
from botocore.exceptions import ClientError
from jsonschema import validate, ValidationError, SchemaError
from okdata.aws.logging import log_add, log_exception, logging_wrapper
from okdata.resource_auth import ResourceAuthorizer
from okdata.sdk.data.dataset import Dataset

from uploader.common import (
    error_response,
    generate_s3_path,
    get_and_validate_dataset,
    sdk_config,
)
from uploader.dataset import append_to_dataset
from uploader.errors import (
    InvalidSourceTypeError,
    DatasetNotFoundError,
    InvalidTypeError,
)
from uploader.schema import get_model_schema

patch_all()

LOCK_WAIT_SECONDS = 5
LOCK_RETRIES = 5

resource_authorizer = ResourceAuthorizer()


def _handle_events(dataset, version, source_s3_path, events):
    dataset_id = dataset["Id"]

    merged_data = append_to_dataset(source_s3_path, events)
    sdk = Dataset(sdk_config())
    edition = sdk.auto_create_edition(dataset_id, version)

    target_s3_path_processed = generate_s3_path(
        dataset, edition["Id"], "processed", absolute=True
    )
    target_s3_path_raw = generate_s3_path(dataset, edition["Id"], "raw")

    log_add(
        target_s3_path_processed=target_s3_path_processed,
        target_s3_path_raw=target_s3_path_raw,
    )

    # Write the raw input data
    s3 = boto3.client("s3", region_name=os.environ["AWS_REGION"])
    s3.put_object(
        Body=json.dumps(events),
        Bucket=os.environ["BUCKET"],
        Key=f"{target_s3_path_raw}/data.json",
    )

    # Clean out any existing data in `latest`
    wr.s3.delete_objects(source_s3_path)

    # Write merged data to both the new edition and to `latest`
    for path in target_s3_path_processed, source_s3_path:
        wr.s3.to_deltalake(
            df=merged_data,
            path=path,
            mode="overwrite",
            schema_mode="merge",
            s3_allow_unsafe_rename=True,
        )

    # Create new distribution
    edition_id = edition["Id"]
    log_add(edition_id=edition_id)

    distribution = sdk.create_distribution(
        dataset_id,
        version,
        edition_id.split("/")[2],
        data={
            "distribution_type": "file",
            "content_type": "application/vnd.apache.parquet",
            "filenames": [
                obj.removeprefix(f"{source_s3_path}/")
                for obj in wr.s3.list_objects(source_s3_path)
            ],
        },
        retries=3,
    )

    log_add(distribution_id=distribution["Id"])

    return edition["Id"]


@logging_wrapper
@xray_recorder.capture("push_dataset_events")
def handler(event, context):
    try:
        body = json.loads(event["body"])
        validate(body, get_model_schema("pushEventsRequest"))
        dataset_id = body["datasetId"]
        version = body.get("version", "1")

        log_add(
            dataset_id=dataset_id,
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
            lock_table.put_item(
                Item={
                    "DatasetId": dataset_id,
                    "Timestamp": datetime.now(timezone.utc).isoformat(),
                },
                ConditionExpression="attribute_not_exists(DatasetId)",
            )
            locked = True
            edition_id = _handle_events(
                dataset, version, source_s3_path, body["events"]
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                time.sleep(LOCK_WAIT_SECONDS)
                tries += 1
            else:
                raise
        except InvalidTypeError as e:
            log_add(exc_info=e)
            return error_response(400, str(e))
        finally:
            if locked:
                lock_table.delete_item(Key={"DatasetId": dataset_id})

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
