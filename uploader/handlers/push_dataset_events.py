import json
import os
from json.decoder import JSONDecodeError

import awswrangler as wr
import boto3
from aws_xray_sdk.core import patch_all, xray_recorder
from jsonschema import validate, ValidationError, SchemaError

from okdata.aws.logging import logging_wrapper, log_add
from okdata.resource_auth import ResourceAuthorizer
from okdata.sdk.data.dataset import Dataset

from uploader.common import (
    get_and_validate_dataset,
    error_response,
    generate_s3_path,
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

ENABLE_AUTH = os.environ.get("ENABLE_AUTH", "false") == "true"

resource_authorizer = ResourceAuthorizer()


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
    except JSONDecodeError as e:
        log_add(exc_info=e)
        return error_response(400, "Body is not a valid JSON document")
    except ValidationError as e:
        log_add(exc_info=e)
        return error_response(400, "JSON document does not conform to the given schema")
    except (SchemaError, Exception) as e:
        log_add(exc_info=e)
        return error_response(500, "Internal server error")

    token = event["headers"]["Authorization"].split(" ")[-1]

    has_access = resource_authorizer.has_access(
        token, "okdata:dataset:write", f"okdata:dataset:{dataset_id}"
    )
    log_add(enable_auth=ENABLE_AUTH, has_access=has_access)

    if ENABLE_AUTH and not has_access:
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

    try:
        merged_data = append_to_dataset(source_s3_path, body["events"])
    except InvalidTypeError as e:
        log_add(exc_info=e)
        return error_response(400, str(e))

    edition = Dataset(sdk_config()).auto_create_edition(dataset_id, version)

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
        Body=json.dumps(body["events"]),
        Bucket=os.environ["BUCKET"],
        Key=f"{target_s3_path_raw}/data.json",
    )

    # Write merged data to both the new edition and to `latest`
    for path in target_s3_path_processed, source_s3_path:
        wr.s3.to_deltalake(
            df=merged_data,
            path=path,
            mode="overwrite",
            schema_mode="merge",
            s3_allow_unsafe_rename=True,
        )

    return {
        "statusCode": 200,
        "body": "",
    }
