import os
import json
import logging

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

log = logging.getLogger()
log.setLevel(logging.INFO)

BUCKET = os.environ["BUCKET"]


def handler(event, context):
    # TODO: Proper auth
    if event["requestContext"]["authorizer"]["principalId"] != "jd":
        return error_response(403, "Forbidden. Only the test user can do this")

    body = None
    try:
        body = json.loads(event["body"])
        validate(body, request_schema)
    except JSONDecodeError as e:
        log.exception(f"Body is not a valid JSON document: {e}")
        return error_response(400, "Body is not a valid JSON document")
    except ValidationError as e:
        log.exception(f"JSON document does not conform to the given schema: {e}")
        return error_response(400, "JSON document does not conform to the given schema")
    except SchemaError as e:
        log.exception(f"Schema error: {e}")
        return error_response(500, "Internal server error")

    try:
        editionId = body["editionId"]
        edition_created = False
        if edition_missing(editionId) and validate_version(editionId):
            body["editionId"] = create_edition(editionId)
            edition_created = True

        if not edition_created and not validate_edition(editionId):
            raise InvalidDatasetEditionError()
    except InvalidDatasetEditionError:
        log.exception(f"Trying to insert invalid dataset edition: {body}")
        return error_response(403, "Incorrect dataset edition")
    except DataExistsError as e:
        log.exception(f"Data already exists: {e}")
        return error_response(400, "Could not create data as resource already exists")
    except Exception as e:
        log.exception(f"Unexpected Exception found : {e}")
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
