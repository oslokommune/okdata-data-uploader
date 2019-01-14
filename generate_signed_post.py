import json
import boto3

auth_error = {
    "message": "Forbidden. Only the test user can do this",
}

def handler(event, context):
    # TODO: Proper auth
    if event['requestContext']['authorizer']['principalId'] != "test-dataplatform":
        return {
            "isBase64Encoded": False,
            "statusCode": 403,
            "body": json.dumps(auth_error),
        }

    body = json.loads(event['body'])
    distributionId = body['distributionId']
    # TODO: Magically resolve bucket and key
    distribution = {
        'bucket': 'ok-origo-dataplatform-testbucket',
        'key': f"data-upload-test/{distributionId}",
    }

    post_response = generate_signed_post(distribution['bucket'], distribution['key'])

    return {
        "isBase64Encoded": False,
        "statusCode": 200,
        "body": json.dumps(post_response)
    }

def generate_signed_post(bucket, key):
    s3 = boto3.client('s3')
    return s3.generate_presigned_post(bucket, key)
