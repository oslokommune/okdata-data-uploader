import os
import json

from aws_xray_sdk.core import patch_all, xray_recorder
from dataplatform.awslambda.logging import logging_wrapper, log_add
from jsonschema import validate, ValidationError, SchemaError
from json.decoder import JSONDecodeError

from uploader.common import (
    dataset_exist,
    error_response,
    edition_missing,
    validate_edition,
    validate_version,
    create_edition,
    generate_s3_path,
    generate_signed_post,
    generate_post_for_status_api,
)
from uploader.errors import DataExistsError, InvalidDatasetEditionError
from uploader.schema import request_schema
from auth import SimpleAuth

patch_all()

BUCKET = os.environ["BUCKET"]
ENABLE_AUTH = os.environ.get("ENABLE_AUTH", "false") == "true"


@logging_wrapper("data-uploader")
@xray_recorder.capture("generate_signed_post")
def handler(event, context):
    body = None
    try:
        body = json.loads(event["body"])
        validate(body, request_schema)
    except JSONDecodeError as e:
        log_add(exc_info=e)
        return error_response(400, "Body is not a valid JSON document")
    except ValidationError as e:
        log_add(exc_info=e)
        return error_response(400, "JSON document does not conform to the given schema")
    except SchemaError as e:
        log_add(exc_info=e)
        return error_response(500, "Internal server error")

    log_add(filename=body["filename"], edition_id=body["editionId"])

    maybe_edition = body["editionId"]
    dataset_id, *_ = maybe_edition.split("/")

    if not dataset_exist(dataset_id):
        return error_response(404, f"Dataset {dataset_id} does not exist")

    is_owner = SimpleAuth().is_owner(event, dataset_id)
    log_add(enable_auth=ENABLE_AUTH, is_owner=is_owner)

    if ENABLE_AUTH and not is_owner:
        return error_response(403, "Forbidden")

    try:
        edition_created = False
        if edition_missing(maybe_edition) and validate_version(maybe_edition):
            body["editionId"] = create_edition(event, maybe_edition)
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

    s3path = generate_s3_path(**body)
    log_add(generated_s3_path=s3path)

    status_response = generate_post_for_status_api(event, s3path, dataset_id)

    post_response = generate_signed_post(BUCKET, s3path)
    post_response["status_response"] = status_response

    log_add(full_post_response=post_response)

    return {
        "isBase64Encoded": False,
        "statusCode": 200,
        "headers": {"Access-Control-Allow-Origin": "*"},
        "body": json.dumps(post_response),
    }
