import requests
import os
import json
import boto3
from botocore.client import Config


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


def error_response(status, message):
    return {
        "isBase64Encoded": False,
        "statusCode": status,
        "body": json.dumps({"message": message}),
    }


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


def validate_edition(editionId):
    parts = editionId.split("/")
    baseUrl = os.environ["METADATA_API"]
    # If this URL exists and the data there matches what we get in from
    # erditionId, then we know that editionId has been created by the metadata API
    url = f"{baseUrl}/datasets/{parts[0]}/versions/{parts[1]}/editions/{parts[2]}"
    response = requests.get(url)
    data = response.json()
    if "Id" in data and editionId == data["Id"]:
        return True

    return False
