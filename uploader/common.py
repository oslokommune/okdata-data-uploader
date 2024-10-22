import requests
import os
import json
import boto3
import uuid

from okdata.aws.logging import log_duration
from uploader.errors import (
    DataExistsError,
    DatasetNotFoundError,
    InvalidDatasetEditionError,
    InvalidSourceTypeError,
)
from botocore.client import Config as BotoConfig
from datetime import datetime

import functools

from okdata.aws.ssm import get_secret
from okdata.sdk.config import Config

BASE_URL = os.environ["METADATA_API_URL"]
STATUS_API_URL = os.environ["STATUS_API_URL"]

CONFIDENTIALITY_MAP = {
    "public": "green",
    "restricted": "yellow",
    "non-public": "red",
}


def generate_s3_path(
    dataset_metadata: dict,
    edition_id: str,
    stage: str = "raw",
    filename: str | None = None,
    absolute: bool = False,
):
    dataset_id, version, edition = edition_id.split("/")
    confidentiality = get_confidentiality(dataset_metadata)
    s3_dataset_path_prefix = f"{stage}/{confidentiality}"

    if dataset_metadata.get("parent_id", None):
        parent_path = f"{dataset_metadata['parent_id']}"
        s3_dataset_path_prefix = f"{s3_dataset_path_prefix}/{parent_path}"

    if edition != "latest":
        edition = f"edition={edition}"

    path = [s3_dataset_path_prefix, dataset_id, f"version={version}", edition]

    if filename:
        path.append(filename)

    if absolute:
        bucket_name = os.environ["BUCKET"]
        path = [f"s3://{bucket_name}"] + path

    return "/".join(path)


def generate_signed_post(bucket, key):
    # Path adressing style (which needs region specified) used because CORS doesn't propagate on global URIs immediately
    s3 = boto3.client(
        "s3",
        region_name=os.environ["AWS_REGION"],
        config=BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"}),
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


def get_and_validate_dataset(dataset_id, source_type="file"):
    url = f"{BASE_URL}/datasets/{dataset_id}"
    response = requests.get(url)

    if response.status_code == 404:
        raise DatasetNotFoundError

    response.raise_for_status()

    dataset = response.json()
    dataset_source_type = dataset["source"]["type"]

    if source_type != dataset_source_type:
        error_msg = f"Invalid source.type '{dataset_source_type}' for dataset: {dataset_id}. Must be source.type='{source_type}'"
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


def split_edition_id(edition_id):
    """Extract dataset ID and version from `edition_id`.

    Raise `InvalidDatasetEditionError` if `edition_id` doesn't look like an
    edition ID.
    """
    try:
        dataset_id, version, _edition, *_ = edition_id.split("/")
    except ValueError:
        raise InvalidDatasetEditionError

    return dataset_id, version


@functools.cache
def sdk_config():
    config = Config()
    config.config["client_secret"] = get_secret(
        "/dataplatform/data-uploader/keycloak-client-secret"
    )
    return config
