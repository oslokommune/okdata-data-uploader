import json
import requests_mock

from uploader.common import (
    validate_edition,
    validate_version,
    edition_missing,
    create_edition,
)
from uploader.errors import DataExistsError


@requests_mock.Mocker(kw="mock")
def test_validate_edition_correct_edition(**kwargs):
    url = "https://metadata.api-test.oslo.kommune.no/dev/datasets/h-eide-test2-5C5uX/versions/1/editions/20190528T133700"
    response = json.dumps({"Id": "h-eide-test2-5C5uX/1/20190528T133700"})
    kwargs["mock"].register_uri("GET", url, text=response, status_code=200)
    editionId = "h-eide-test2-5C5uX/1/20190528T133700"
    result = validate_edition(editionId)
    assert result is True


@requests_mock.Mocker(kw="mock")
def test_validate_edition_wrong_edition(**kwargs):
    url = "https://metadata.api-test.oslo.kommune.no/dev/datasets/h-eide-test2-5C5uX/versions/1/editions/20190528T133700"
    response = json.dumps({"Id": "incorrect/1/20190528T133700"})
    kwargs["mock"].register_uri("GET", url, text=response, status_code=200)
    editionId = "h-eide-test2-5C5uX/1/20190528T133700"
    result = validate_edition(editionId)
    assert result is False


@requests_mock.Mocker(kw="mock")
def test_validate_version_correct_edition(**kwargs):
    url = "https://metadata.api-test.oslo.kommune.no/dev/datasets/h-eide-test2-5C5uX/versions/1"
    response = json.dumps({"Id": "h-eide-test2-5C5uX/1"})
    kwargs["mock"].register_uri("GET", url, text=response, status_code=200)
    editionId = "h-eide-test2-5C5uX/1"
    result = validate_version(editionId)
    assert result is True


@requests_mock.Mocker(kw="mock")
def test_validate_version_wrong_edition(**kwargs):
    url = "https://metadata.api-test.oslo.kommune.no/dev/datasets/h-eide-test2-5C5uX/versions/1"
    response = json.dumps({"Id": "incorrect/1"})
    kwargs["mock"].register_uri("GET", url, text=response, status_code=200)
    editionId = "h-eide-test2-5C5uX/1"
    result = validate_version(editionId)
    assert result is False


@requests_mock.Mocker(kw="mock")
def test_create_edition(**kwargs):
    url = "https://metadata.api-test.oslo.kommune.no/dev/h-eide-test2-5C5uX/versions/1/editions"
    response = "h-eide-test2-5C5uX/versions/1/editions/2019-08-01T12:00:00"
    kwargs["mock"].register_uri("POST", url, text=response, status_code=200)
    editionId = "h-eide-test2-5C5uX/1"
    result = create_edition(editionId)
    assert result == "h-eide-test2-5C5uX/versions/1/editions/2019-08-01T12:00:00"


@requests_mock.Mocker(kw="mock")
def test_create_edition_exists(**kwargs):
    url = "https://metadata.api-test.oslo.kommune.no/dev/h-eide-test2-5C5uX/versions/1/editions"
    response = "h-eide-test2-5C5uX/versions/1/editions/2019-08-01T12:00:00"
    kwargs["mock"].register_uri("POST", url, text=response, status_code=409)
    try:
        editionId = "h-eide-test2-5C5uX/1"
        create_edition(editionId)
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
