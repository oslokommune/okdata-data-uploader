import json

from botocore.vendored import requests

METADATA_API_URL = "https://***REMOVED***.execute-api.eu-west-1.amazonaws.com/dev"


def handler(event, context):
    print("Event", event)

    path = event["pathParameters"]["path"]
    method = event["httpMethod"]

    req = None
    if method == "POST":
        body = json.loads(event["body"])
        req = requests.post(f"{METADATA_API_URL}/{path}", json=body)
    elif method == "GET":
        req = requests.get(f"{METADATA_API_URL}/{path}")
    else:
        return error_response(400, "Nope")

    return {"isBase64Encoded": False, "statusCode": 200, "body": req.text}


def error_response(status, message):
    return {
        "isBase64Encoded": False,
        "statusCode": status,
        "body": json.dumps({"message": message}),
    }
