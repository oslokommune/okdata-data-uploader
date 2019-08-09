import os
import json
import boto3
import logging
import requests

from botocore.client import Config
from jsonschema import validate, ValidationError, SchemaError
from json.decoder import JSONDecodeError

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

    # TODO: Should we verify dataset/schema here, or just let the frontend do it?
    # TODO: Same with creating edition; here or in frontend?
    s3path = generate_s3_path(**body)
    log.info(f"S3 key: {s3path}")
    post_response = generate_signed_post(bucket, s3path)

    return {
        "isBase64Encoded": False,
        "statusCode": 200,
        "headers": {"Access-Control-Allow-Origin": "*"},
        "body": json.dumps(post_response),
    }


def get_confidentiality(dataset):
    baseUrl = os.environ["METADATA_API"]
    url = f"{baseUrl}/datasets/{dataset}"
    response = requests.get(url)
    data = response.json()
    confidentiality = "green"
    if "confidentiality" in data:
        confidentiality = data["confidentiality"]
    return confidentiality


def generate_s3_path(editionId, filename):
    dataset = editionId.split("/")[0]
    version = editionId.split("/")[1]
    edition = editionId.split("/")[2]
    confidentiality = get_confidentiality(dataset)
    return f"incoming/{confidentiality}/{dataset}/version={version}/edition={edition}/{filename}"


def generate_signed_post(bucket, key):
    # Path adressing style (which needs region specified) used because CORS doesn't propagate on global URIs immediately
    s3 = boto3.client(
        "s3",
        region_name="eu-west-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )

    # TODO: Add more conditions!
    fields = {"acl": "private"}
    conditions = [{"acl": "private"}]

    return s3.generate_presigned_post(
        bucket, key, Fields=fields, Conditions=conditions, ExpiresIn=300
    )


def error_response(status, message):
    return {
        "isBase64Encoded": False,
        "statusCode": status,
        "body": json.dumps({"message": message}),
    }
