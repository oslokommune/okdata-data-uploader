import json
import os
from json.decoder import JSONDecodeError

import awswrangler as wr
import pandas as pd
from aws_xray_sdk.core import patch_all, xray_recorder
from deltalake.exceptions import TableNotFoundError
from jsonschema import validate, ValidationError, SchemaError

from okdata.aws.logging import logging_wrapper, log_add
from okdata.resource_auth import ResourceAuthorizer

from uploader.common import (
    get_and_validate_dataset,
    error_response,
    generate_s3_path,
)
from uploader.errors import (
    InvalidSourceTypeError,
    DatasetNotFoundError,
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

    relative_path = generate_s3_path(
        dataset, f"{dataset_id}/{version}/latest", "processed"
    )
    bucket_name = os.environ["BUCKET"]
    s3_path = f"s3://{bucket_name}/{relative_path}"

    log_add(s3_path=s3_path)

    new_data = pd.DataFrame.from_dict(body["events"])

    try:
        existing_dataset = wr.s3.read_deltalake(s3_path, dtype_backend="pyarrow")
        merged_data = pd.concat([existing_dataset, new_data])
    except TableNotFoundError:
        merged_data = new_data

    # Ensure that we have no index
    merged_data = merged_data.reset_index(drop=True)

    return {}

    # 1. Get latest edition data from processed (if exists)
    # 1.1 Create Delta Lake dataset
    # 2. Attempt add data
    # 2.0 Success: Write input events as edition in `raw`
    # 2.1 Success: Write dataset to new edition in `processed`
