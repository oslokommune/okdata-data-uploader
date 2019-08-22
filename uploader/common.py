import requests
import os
import json
import boto3

from uploader.errors import DataExistsError
from botocore.client import Config
from datetime import datetime

BASE_URL = os.environ["METADATA_API"]


def generate_s3_path(editionId, filename):
    dataset, version, edition = editionId.split("/")
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


def get_confidentiality(dataset):
    url = f"{BASE_URL}/datasets/{dataset}"
    response = requests.get(url)
    data = response.json()
    confidentiality = "green"
    if "confidentiality" in data:
        confidentiality = data["confidentiality"]
    return confidentiality


def validate_edition(editionId):
    dataset, version, edition = editionId.split("/")
    # If this URL exists and the data there matches what we get in from
    # erditionId, then we know that editionId has been created by the metadata API
    url = f"{BASE_URL}/datasets/{dataset}/versions/{version}/editions/{edition}"
    response = requests.get(url)
    data = response.json()
    if "Id" in data and editionId == data["Id"]:
        return True

    return False


def validate_version(editionId):
    dataset, version = editionId.split("/")
    url = f"{BASE_URL}/datasets/{dataset}/versions/{version}"
    response = requests.get(url)
    data = response.json()
    if "Id" in data and editionId == data["Id"]:
        return True

    return False


def edition_missing(editionId):
    parts = editionId.split("/")
    if len(parts) == 2:
        return True
    if len(parts) == 3 and parts[2] == "":
        return True

    return False


def create_edition(editionId):
    dataset, version = editionId.split("/")
    edition = datetime.now().isoformat(timespec="seconds")
    data = {"edition": edition, "description": f"Data for {edition}"}
    url = f"{BASE_URL}/{dataset}/versions/{version}/editions"
    result = requests.post(url, data=json.dumps(data))
    if result.status_code == 409:
        edition = data["edition"]
        raise DataExistsError(
            f"Edition: {edition} on datasetId {dataset} already exists"
        )

    id = result.text.replace('"', "")
    return id