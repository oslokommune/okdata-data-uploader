import os
import json
import logging

from jsonschema import validate, ValidationError, SchemaError
from json.decoder import JSONDecodeError
from common import (
    error_response,
    validate_edition,
    generate_s3_path,
    generate_signed_post,
)

log = logging.getLogger()

request_schema = None
response_schema = None

with open("doc/models/uploadRequest.json") as f:
    request_schema = json.loads(f.read())

with open("doc/models/uploadResponse.json") as f:
    response_schema = json.loads(f.read())


def handler(event, context):
    # TODO: Proper auth
    if event["requestContext"]["authorizer"]["principalId"] != "jd":
        return error_response(403, "Forbidden. Only the test user can do this")

    bucket = os.environ["BUCKET"]

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

    if validate_edition(body["editionId"]) is False:
        return error_response(403, "Incorrect dataset edition")

    s3path = generate_s3_path(**body)
    log.info(f"S3 key: {s3path}")
    post_response = generate_signed_post(bucket, s3path)

    return {
        "isBase64Encoded": False,
        "statusCode": 200,
        "headers": {"Access-Control-Allow-Origin": "*"},
        "body": json.dumps(post_response),
    }
