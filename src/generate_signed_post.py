import os
import json
import boto3

from botocore.client import Config
from jsonschema import validate, ValidationError, SchemaError
from json.decoder import JSONDecodeError


request_schema = None
response_schema = None

with open("doc/models/uploadRequest.json") as f:
    request_schema = json.loads(f.read())

with open("doc/models/uploadResponse.json") as f:
    response_schema = json.loads(f.read())


def handler(event, context):
    # TODO: Proper auth
    if event["requestContext"]["authorizer"]["principalId"] != "test-dataplatform":
        return error_response(403, "Forbidden. Only the test user can do this")

    bucket = os.environ["BUCKET"]

    body = None
    try:
        body = json.loads(event["body"])
        validate(body, request_schema)
    except JSONDecodeError as e:
        return error_response(400, "Body is not a valid JSON document")
    except ValidationError as e:
        return error_response(400, "JSON document does not conform to the given schema")
    except SchemaError:
        return error_response(500, "Internal server error")

    distributionId = body["distributionId"]
    # TODO: Magically resolve bucket and key
    distribution = {"bucket": bucket, "key": f"data-upload-test/{distributionId}"}

    post_response = generate_signed_post(distribution["bucket"], distribution["key"])

    return {
        "isBase64Encoded": False,
        "statusCode": 200,
        "body": json.dumps(post_response),
    }


def generate_signed_post(bucket, key):
    # Path adressing style (which needs region specified) used because CORS doesn't propagate on global URIs immediately
    s3 = boto3.client(
        "s3",
        region_name="eu-west-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )

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
