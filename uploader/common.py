import requests
import os
import json
import boto3
import uuid

from okdata.aws.logging import log_duration
from uploader.errors import (
    DataExistsError,
    DatasetNotFoundError,
    InvalidSourceTypeError,
)
from botocore.client import Config
from datetime import datetime


BASE_URL = os.environ["METADATA_API_URL"]
STATUS_API_URL = os.environ["STATUS_API_URL"]
AUTHORIZER_API = os.environ["AUTHORIZER_API"]

CONFIDENTIALITY_MAP = {
    "public": "green",
    "restricted": "yellow",
    "non-public": "red",
}


def generate_s3_path(dataset: dict, edition_id: str, filename: str):
    dataset_id, version, edition = edition_id.split("/")
    confidentiality = get_confidentiality(dataset)
    s3_dataset_path_prefix = f"raw/{confidentiality}"
    if dataset.get("parent_id", None):
        parent_path = f"{dataset['parent_id']}"
        s3_dataset_path_prefix = f"{s3_dataset_path_prefix}/{parent_path}"
    return f"{s3_dataset_path_prefix}/{dataset_id}/version={version}/edition={edition}/{filename}"


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

    presigned_post = log_duration(
        lambda: s3.generate_presigned_post(
            bucket, key, Fields=fields, Conditions=conditions, ExpiresIn=300
        ),
        "duration_generate_presigned_post",
    )
    return presigned_post


def generate_uuid(s3path, dataset_id):
    new_uuid = uuid.uuid4()
    return f"{dataset_id}-{new_uuid}"[0:80]


def create_status_trace(token, status_data):
    response = requests.post(
        STATUS_API_URL,
        json.dumps(status_data),
        headers={
            "Authorization": f"Bearer {token}",
        },
    )
    return response.json()


def error_response(status, message):
    return {
        "isBase64Encoded": False,
        "statusCode": status,
        "headers": {"Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"message": message}),
    }


def get_and_validate_dataset(dataset_id):
    url = f"{BASE_URL}/datasets/{dataset_id}"
    response = requests.get(url)

    if response.status_code == 404:
        raise DatasetNotFoundError

    response.raise_for_status()

    dataset = response.json()
    source_type = dataset["source"]["type"]

    if source_type != "file":
        error_msg = f"Invalid source.type '{source_type}' for dataset: {dataset_id}. Must be source.type='file'"
        raise InvalidSourceTypeError(error_msg)

    return dataset


def get_confidentiality(data):
    try:
        return CONFIDENTIALITY_MAP[data["accessRights"]]
    except KeyError:
        raise ValueError("Invalid `accessRights`")


def validate_edition(editionId):
    try:
        dataset_id, version, edition = editionId.split("/")
    except ValueError:
        return False
    if not all([dataset_id, version, edition]):
        return False
    # If this URL exists and the data there matches what we get in from
    # erditionId, then we know that editionId has been created by the metadata API
    url = f"{BASE_URL}/datasets/{dataset_id}/versions/{version}/editions/{edition}"
    response = log_duration(lambda: requests.get(url), "requests_validate_edition_ms")
    data = response.json()
    if "Id" in data and editionId == data["Id"]:
        return True

    return False


def validate_version(editionId):
    dataset_id, version = editionId.split("/")
    url = f"{BASE_URL}/datasets/{dataset_id}/versions/{version}"
    response = log_duration(lambda: requests.get(url), "requests_validate_version_ms")
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


def create_edition(token, editionId):
    dataset_id, version = editionId.split("/")
    edition = datetime.now().isoformat(timespec="seconds")
    data = {"edition": edition, "description": f"Data for {edition}"}
    url = f"{BASE_URL}/{dataset_id}/versions/{version}/editions"
    result = requests.post(
        url,
        data=json.dumps(data),
        headers={
            "Authorization": f"Bearer {token}",
        },
    )
    if result.status_code == 409:
        edition = data["edition"]
        raise DataExistsError(
            f"Edition: {edition} on datasetId {dataset_id} already exists"
        )

    id = result.text.replace('"', "")
    return id


def is_dataset_owner(token, dataset_id):
    result = requests.get(
        f"{AUTHORIZER_API}/{dataset_id}", headers={"Authorization": f"Bearer {token}"}
    )
    result.raise_for_status()
    data = result.json()
    return "access" in data and data["access"]
