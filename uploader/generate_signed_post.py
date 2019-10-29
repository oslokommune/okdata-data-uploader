import os
import json
import logging

from dataplatform.awslambda.logging import logging_wrapper, log_add
from jsonschema import validate, ValidationError, SchemaError
from json.decoder import JSONDecodeError

from uploader.common import (
    error_response,
    edition_missing,
    validate_edition,
    validate_version,
    create_edition,
    generate_s3_path,
    generate_signed_post,
)
from uploader.errors import DataExistsError, InvalidDatasetEditionError
from uploader.schema import request_schema
from auth import SimpleAuth

log = logging.getLogger()
log.setLevel(logging.INFO)

BUCKET = os.environ["BUCKET"]
ENABLE_AUTH = os.environ.get("ENABLE_AUTH", "false") == "true"


@logging_wrapper
def handler(event, context):
    body = None
    try:
        body = json.loads(event["body"])
        validate(body, request_schema)
    except JSONDecodeError as e:
        log.exception(f"Body is not a valid JSON document: {e}")
        log_add(validate_decode_error=e)
        return error_response(400, "Body is not a valid JSON document")
    except ValidationError as e:
        log.exception(f"JSON document does not conform to the given schema: {e}")
        return error_response(400, "JSON document does not conform to the given schema")
    except SchemaError as e:
        log.exception(f"Schema error: {e}")
        return error_response(500, "Internal server error")

    editionId = body["editionId"]
    dataset, *_ = editionId.split("/")

    log.info(f"Upload to {editionId}")
    log_add(edition_id=editionId)
    if ENABLE_AUTH and not SimpleAuth().is_owner(event, dataset):
        log.info("Access denied")
        msg = "Access denied - Forbidden"
        log_add(simpleAuth_error=msg)
        return error_response(403, "Forbidden")

    try:

        edition_created = False
        if edition_missing(editionId) and validate_version(editionId):
            body["editionId"] = create_edition(event, editionId)
            edition_created = True

        if not edition_created and not validate_edition(editionId):
            raise InvalidDatasetEditionError()
    except InvalidDatasetEditionError:
        log.exception(f"Trying to insert invalid dataset edition: {body}")
        log_add(dataset_invalid_edition=body)
        return error_response(403, "Incorrect dataset edition")
    except DataExistsError as e:
        log.exception(f"Data already exists: {e}")
        log_add(dataset_already_exist=e)
        return error_response(400, "Could not create data as resource already exists")
    except Exception as e:
        log.exception(f"Unexpected Exception found : {e}")
        log_add(dataset_unexpected_exeption=e)
        return error_response(400, "Could not complete request, please try again later")

    s3path = generate_s3_path(**body)
    log.info(f"S3 key: {s3path}")

    post_response = generate_signed_post(BUCKET, s3path)

    return {
        "isBase64Encoded": False,
        "statusCode": 200,
        "headers": {"Access-Control-Allow-Origin": "*"},
        "body": json.dumps(post_response),
    }
