import os
import json

from aws_xray_sdk.core import patch_all, xray_recorder
from okdata.aws.logging import logging_wrapper, log_add
from jsonschema import validate, ValidationError, SchemaError
from json.decoder import JSONDecodeError
from datetime import datetime, timezone

from uploader.common import (
    get_and_validate_dataset,
    is_dataset_owner,
    error_response,
    edition_missing,
    validate_edition,
    validate_version,
    create_edition,
    generate_s3_path,
    generate_signed_post,
    create_status_trace,
)
from uploader.errors import (
    DataExistsError,
    InvalidDatasetEditionError,
    InvalidSourceTypeError,
    DatasetNotFoundError,
)
from uploader.schema import request_schema

patch_all()

BUCKET = os.environ["BUCKET"]
ENABLE_AUTH = os.environ.get("ENABLE_AUTH", "false") == "true"


@logging_wrapper
@xray_recorder.capture("generate_signed_post")
def handler(event, context):
    try:
        body = json.loads(event["body"])
        validate(body, request_schema)

        log_add(filename=body["filename"], edition_id=body["editionId"])
        maybe_edition = body["editionId"]

        dataset_id, dataset_version, *_ = maybe_edition.split("/")
        log_add(dataset_id=dataset_id)

        dataset = get_and_validate_dataset(dataset_id)
    except JSONDecodeError as e:
        log_add(exc_info=e)
        return error_response(400, "Body is not a valid JSON document")
    except ValidationError as e:
        log_add(exc_info=e)
        return error_response(400, "JSON document does not conform to the given schema")
    except InvalidSourceTypeError as e:
        return error_response(
            400,
            str(e),
        )
    except DatasetNotFoundError:
        return error_response(404, f"Dataset {dataset_id} does not exist")
    except (SchemaError, Exception) as e:
        log_add(exc_info=e)
        return error_response(500, "Internal server error")

    token = event["headers"]["Authorization"].split(" ")[-1]

    is_owner = is_dataset_owner(token, dataset_id)
    log_add(enable_auth=ENABLE_AUTH, is_owner=is_owner)

    if ENABLE_AUTH and not is_owner:
        return error_response(403, "Forbidden")

    try:
        edition_created = False
        if edition_missing(maybe_edition) and validate_version(maybe_edition):
            body["editionId"] = create_edition(token, maybe_edition)
            edition_created = True

        log_add(edition_id=body["editionId"], edition_created=edition_created)

        if not edition_created and not validate_edition(maybe_edition):
            raise InvalidDatasetEditionError()

    except InvalidDatasetEditionError:
        return error_response(400, "Incorrect dataset edition")
    except DataExistsError:
        return error_response(409, "Could not create data as resource already exists")
    except Exception as e:
        log_add(exc_info=e)
        return error_response(500, "Could not complete request, please try again later")

    try:
        s3_path = generate_s3_path(
            dataset=dataset, edition_id=body["editionId"], filename=body["filename"]
        )
    except ValueError as e:
        return error_response(400, str(e))

    log_add(generated_s3_path=s3_path)

    # TODO: Use status-client from common-python once it allows for creating
    # new traces (not just updating), 2020-11-03, ref.
    # https://github.oslo.kommune.no/origo-dataplatform/common-python/pull/48#discussion_r64632
    principal_id = event["requestContext"]["authorizer"]["principalId"]

    status_data = {
        "domain": "dataset",
        "domain_id": f"{dataset_id}/{dataset_version}",
        "component": "data-uploader",
        "operation": "upload",
        "user": principal_id,
        "start_time": datetime.now(timezone.utc).isoformat(),
        "s3_path": s3_path,
    }

    post_response = generate_signed_post(BUCKET, s3_path)

    status_data["end_time"] = datetime.now(timezone.utc).isoformat()

    status_response = create_status_trace(token, status_data)

    post_response["status_response"] = status_response.get("trace_id")
    post_response["trace_id"] = status_response.get("trace_id")

    log_add(full_post_response=post_response)

    return {
        "isBase64Encoded": False,
        "statusCode": 200,
        "headers": {"Access-Control-Allow-Origin": "*"},
        "body": json.dumps(post_response),
    }
