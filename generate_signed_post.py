import json
import boto3
from botocore.client import Config
import os


auth_error = {
    "message": "Forbidden. Only the test user can do this",
}

headers = {
#    "Access-Control-Allow-Origin": "https://s3-eu-west-1.amazonaws.com",
#    "Access-Control-Allow-Credentials": True,
}

def handler(event, context):
    # TODO: Proper auth
    if event['requestContext']['authorizer']['principalId'] != "test-dataplatform":
        return {
            "isBase64Encoded": False,
            "statusCode": 403,
            "headers": headers,
            "body": json.dumps(auth_error),
        }

    bucket = os.environ["BUCKET"]

    body = json.loads(event['body'])
    distributionId = body['distributionId']
    # TODO: Magically resolve bucket and key
    distribution = {
        'bucket': bucket,
        'key': f"data-upload-test/{distributionId}",
    }

    post_response = generate_signed_post(distribution['bucket'], distribution['key'])

    return {
        "isBase64Encoded": False,
        "statusCode": 200,
        "headers": headers,
        "body": json.dumps(post_response)
    }

def generate_signed_post(bucket, key):
    # Path adressing style (which needs region specified) used because CORS doesn't propagate on global URIs immediately
    s3 = boto3.client('s3', region_name='eu-west-1', config=Config(s3={'addressing_style': 'path'}))

    fields = {
        "acl": "private",
    }
    conditions = [
        {"acl": "private"},
    ]

    return s3.generate_presigned_post(bucket, key, Fields=fields, Conditions=conditions, ExpiresIn=300)
