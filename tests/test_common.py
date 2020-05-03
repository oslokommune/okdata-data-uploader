import json

from uploader.common import (
    get_dataset,
    get_confidentiality,
    validate_edition,
    validate_version,
    edition_missing,
    create_edition,
    generate_s3_path,
)
from uploader.errors import DataExistsError


def test_validate_confidentiality_red(requests_mock):
    url = "https://metadata.api-test.oslo.kommune.no/dev/datasets/confidentiality-red"
    response = json.dumps({"confidentiality": "red"})
    requests_mock.register_uri("GET", url, text=response, status_code=200)
    datasetId = "confidentiality-red"
    dataset_data = get_dataset(datasetId)
    result = get_confidentiality(dataset_data)
    assert result == "red"


def test_validate_confidentiality_green(requests_mock):
    url = "https://metadata.api-test.oslo.kommune.no/dev/datasets/confidentiality-green"
    response = json.dumps({"confidentiality": "green"})
    requests_mock.register_uri("GET", url, text=response, status_code=200)
    datasetId = "confidentiality-green"
    dataset_data = get_dataset(datasetId)
    result = get_confidentiality(dataset_data)
    assert result == "green"


def test_validate_confidentiality_empty(requests_mock):
    url = "https://metadata.api-test.oslo.kommune.no/dev/datasets/confidentiality-empty"
    response = json.dumps({})
    requests_mock.register_uri("GET", url, text=response, status_code=200)
    datasetId = "confidentiality-empty"
    dataset_data = get_dataset(datasetId)
    result = get_confidentiality(dataset_data)
    assert result == "green"


def test_generate_s3_path_parent_id_not_in_upload_path(requests_mock):
    url = "https://metadata.api-test.oslo.kommune.no/dev/datasets/my-dataset"
    response = json.dumps({"confidentiality": "green"})
    requests_mock.register_uri("GET", url, text=response, status_code=200)
    editionId = "my-dataset/1/20200501"
    filename = "hello-world.csv"
    path = generate_s3_path(editionId, filename)
    res = "raw/green/my-dataset/version=1/edition=20200501/hello-world.csv"
    assert path == res


def test_generate_s3_path_parent_id_in_upload_path(requests_mock):
    url = "https://metadata.api-test.oslo.kommune.no/dev/datasets/my-dataset"
    response = json.dumps(
        {"confidentiality": "green", "parent_id": "my-parent-dataset"}
    )
    requests_mock.register_uri("GET", url, text=response, status_code=200)
    editionId = "my-dataset/1/20200501"
    filename = "hello-world.csv"
    path = generate_s3_path(editionId, filename)
    res = "raw/green/my-parent-dataset/my-dataset/version=1/edition=20200501/hello-world.csv"
    assert path == res


def test_validate_edition_correct_edition(requests_mock):
    url = "https://metadata.api-test.oslo.kommune.no/dev/datasets/h-eide-test2-5C5uX/versions/1/editions/20190528T133700"
    response = json.dumps({"Id": "h-eide-test2-5C5uX/1/20190528T133700"})
    requests_mock.register_uri("GET", url, text=response, status_code=200)
    editionId = "h-eide-test2-5C5uX/1/20190528T133700"
    result = validate_edition(editionId)
    assert result is True


def test_validate_edition_wrong_edition(requests_mock):
    url = "https://metadata.api-test.oslo.kommune.no/dev/datasets/h-eide-test2-5C5uX/versions/1/editions/20190528T133700"
    response = json.dumps({"Id": "incorrect/1/20190528T133700"})
    requests_mock.register_uri("GET", url, text=response, status_code=200)
    editionId = "h-eide-test2-5C5uX/1/20190528T133700"
    result = validate_edition(editionId)
    assert result is False


def test_validate_version_correct_edition(requests_mock):
    url = "https://metadata.api-test.oslo.kommune.no/dev/datasets/h-eide-test2-5C5uX/versions/1"
    response = json.dumps({"Id": "h-eide-test2-5C5uX/1"})
    requests_mock.register_uri("GET", url, text=response, status_code=200)
    editionId = "h-eide-test2-5C5uX/1"
    result = validate_version(editionId)
    assert result is True


def test_validate_version_wrong_edition(requests_mock):
    url = "https://metadata.api-test.oslo.kommune.no/dev/datasets/h-eide-test2-5C5uX/versions/1"
    response = json.dumps({"Id": "incorrect/1"})
    requests_mock.register_uri("GET", url, text=response, status_code=200)
    editionId = "h-eide-test2-5C5uX/1"
    result = validate_version(editionId)
    assert result is False


def test_create_edition(requests_mock):
    url = "https://metadata.api-test.oslo.kommune.no/dev/h-eide-test2-5C5uX/versions/1/editions"
    response = "h-eide-test2-5C5uX/versions/1/editions/2019-08-01T12:00:00"
    requests_mock.register_uri("POST", url, text=response, status_code=200)
    editionId = "h-eide-test2-5C5uX/1"
    event = {"headers": {"Authorization": "bearer token"}}
    result = create_edition(event, editionId)
    assert result == "h-eide-test2-5C5uX/versions/1/editions/2019-08-01T12:00:00"


def test_create_edition_exists(requests_mock):
    url = "https://metadata.api-test.oslo.kommune.no/dev/h-eide-test2-5C5uX/versions/1/editions"
    response = "h-eide-test2-5C5uX/versions/1/editions/2019-08-01T12:00:00"
    requests_mock.register_uri("POST", url, text=response, status_code=409)
    event = {"headers": {"Authorization": "bearer token"}}
    try:
        editionId = "h-eide-test2-5C5uX/1"
        create_edition(event, editionId)
    except DataExistsError:
        assert True


def test_edition_missing():
    editionId = "my-dataset/version"
    assert edition_missing(editionId) is True


def test_edition_missing_edition_has_no_value():
    editionId = "my-dataset/version/"
    assert edition_missing(editionId) is True


def test_edition_missing_edition_exists():
    editionId = "my-dataset/version/my-edition"
    assert edition_missing(editionId) is False
