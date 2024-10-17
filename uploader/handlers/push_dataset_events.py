import json
import os
from json.decoder import JSONDecodeError

import awswrangler as wr
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
    MixedTypeError,
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
    except MixedTypeError as e:
        log_add(exc_info=e)
        return error_response(400, str(e))

    edition = Dataset(sdk_config()).auto_create_edition(dataset_id, version)

    target_s3_path = generate_s3_path(
        dataset, edition["Id"], "processed", absolute=True
    )

    log_add(target_s3_path=target_s3_path)

    # wr.s3.to_deltalake(
    #     target_s3_path,
    #     merged_data,
    #     mode="overwrite",
    #     schema_mode="merge",
    #     s3_allow_unsafe_rename=True,
    # )

    # [X] 1. Get latest edition data from processed (if exists)
    # [X] 1.1 Create Delta Lake dataset
    # [X] 2. Attempt add data
    # Må vi støtte arrays/dicts som verdier?
    # Alt "inferres" som datetime (UTC) gitt ISO8601 format - støtte kun date og time?
    # [ ] 2.-1 Create new edition
    # [ ] 2.0 Success: Write input events as edition in `raw`
    # [ ] 2.1 Success: Write dataset to new edition in `processed`

    return {
        "statusCode": 200,
        "body": "",
    }
