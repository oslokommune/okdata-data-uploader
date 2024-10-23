import json

import pytest

from uploader.common import (
    get_and_validate_dataset,
    get_confidentiality,
    validate_edition,
    validate_version,
    edition_missing,
    create_edition,
    generate_s3_path,
)
from uploader.errors import (
    DataExistsError,
    InvalidSourceTypeError,
    DatasetNotFoundError,
)


def test_validate_confidentiality_red():
    confidentiality = get_confidentiality({"accessRights": "non-public"})
    assert confidentiality == "red"


def test_validate_confidentiality_green():
    confidentiality = get_confidentiality({"accessRights": "public"})
    assert confidentiality == "green"


def test_validate_missing_access_rights():
    with pytest.raises(ValueError):
        get_confidentiality({})


def test_generate_s3_path_dataset_without_parent():
    dataset = {"Id": "my-dataset", "accessRights": "public"}
    editionId = "my-dataset/1/20200501"
    filename = "hello-world.csv"
    path = generate_s3_path(dataset, editionId, filename=filename)
    res = "raw/green/my-dataset/version=1/edition=20200501/hello-world.csv"
    assert path == res


def test_generate_s3_path_dataset_with_parent():
    dataset = {
        "Id": "my-dataset",
        "accessRights": "public",
        "parent_id": "my-parent-dataset",
    }
    editionId = "my-dataset/1/20200501"
    filename = "hello-world.csv"
    path = generate_s3_path(dataset, editionId, filename=filename)
    res = "raw/green/my-parent-dataset/my-dataset/version=1/edition=20200501/hello-world.csv"
    assert path == res


def test_generate_s3_path_with_stage():
    dataset = {"Id": "foo", "accessRights": "public"}
    assert (
        generate_s3_path(dataset, "foo/1/bar", stage="processed")
        == "processed/green/foo/version=1/edition=bar"
    )


def test_generate_s3_path_latest_edition():
    dataset = {"Id": "foo", "accessRights": "public"}
    assert generate_s3_path(dataset, "foo/1/latest") == "raw/green/foo/version=1/latest"


def test_generate_s3_path_absolute():
    dataset = {"Id": "foo", "accessRights": "public"}
    assert (
        generate_s3_path(dataset, "foo/1/bar", absolute=True)
        == "s3://testbucket/raw/green/foo/version=1/edition=bar"
    )


def test_generate_s3_path_parent_id_is_null(requests_mock):
    dataset = {"Id": "my-dataset", "accessRights": "public", "parent_id": None}
    editionId = "my-dataset/1/20200501"
    filename = "hello-world.csv"
    path = generate_s3_path(dataset, editionId, filename=filename)
    res = "raw/green/my-dataset/version=1/edition=20200501/hello-world.csv"
    assert path == res


def test_validate_edition_correct_edition(requests_mock):
    url = "https://api.data-dev.oslo.systems/metadata/datasets/h-eide-test2-5C5uX/versions/1/editions/20190528T133700"
    response = json.dumps({"Id": "h-eide-test2-5C5uX/1/20190528T133700"})
    requests_mock.register_uri("GET", url, text=response, status_code=200)
    editionId = "h-eide-test2-5C5uX/1/20190528T133700"
    result = validate_edition(editionId)
    assert result is True


def test_validate_edition_wrong_edition(requests_mock):
    url = "https://api.data-dev.oslo.systems/metadata/datasets/h-eide-test2-5C5uX/versions/1/editions/20190528T133700"
    response = json.dumps({"Id": "incorrect/1/20190528T133700"})
    requests_mock.register_uri("GET", url, text=response, status_code=200)
    editionId = "h-eide-test2-5C5uX/1/20190528T133700"
    result = validate_edition(editionId)
    assert result is False


def test_validate_edition_invalid_id():
    for invalid_edition_id in [
        "h-eide-test2-5C5uX/1",
        "h-eide-test2-5C5uX/1/",
        "h-eide-test2-5C5uX/1/20190528T133700/123",
    ]:
        result = validate_edition(invalid_edition_id)
        assert result is False


def test_validate_version_correct_edition(requests_mock):
    url = "https://api.data-dev.oslo.systems/metadata/datasets/h-eide-test2-5C5uX/versions/1"
    response = json.dumps({"Id": "h-eide-test2-5C5uX/1"})
    requests_mock.register_uri("GET", url, text=response, status_code=200)
    editionId = "h-eide-test2-5C5uX/1"
    result = validate_version(editionId)
    assert result is True


def test_validate_version_wrong_edition(requests_mock):
    url = "https://api.data-dev.oslo.systems/metadata/datasets/h-eide-test2-5C5uX/versions/1"
    response = json.dumps({"Id": "incorrect/1"})
    requests_mock.register_uri("GET", url, text=response, status_code=200)
    editionId = "h-eide-test2-5C5uX/1"
    result = validate_version(editionId)
    assert result is False


def test_create_edition(requests_mock):
    url = "https://api.data-dev.oslo.systems/metadata/h-eide-test2-5C5uX/versions/1/editions"
    response = "h-eide-test2-5C5uX/versions/1/editions/2019-08-01T12:00:00"
    requests_mock.register_uri("POST", url, text=response, status_code=200)
    editionId = "h-eide-test2-5C5uX/1"
    event = {"headers": {"Authorization": "bearer token"}}
    result = create_edition(event, editionId)
    assert result == "h-eide-test2-5C5uX/versions/1/editions/2019-08-01T12:00:00"


def test_create_edition_exists(requests_mock):
    url = "https://api.data-dev.oslo.systems/metadata/h-eide-test2-5C5uX/versions/1/editions"
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


def test_get_and_validate_dataset(requests_mock):
    dataset_id = "my-dataset"
    url = f"https://api.data-dev.oslo.systems/metadata/datasets/{dataset_id}"
    response = json.dumps(
        {"Id": dataset_id, "source": {"type": "file"}, "accessRights": "public"}
    )
    requests_mock.register_uri("GET", url, text=response, status_code=200)

    assert get_and_validate_dataset(dataset_id) == {
        "Id": dataset_id,
        "source": {"type": "file"},
        "accessRights": "public",
    }


def test_get_and_validate_dataset_invalid_source_type(requests_mock):
    dataset_id = "my-dataset"
    invalid_source_type = "event"
    url = f"https://api.data-dev.oslo.systems/metadata/datasets/{dataset_id}"
    response = json.dumps(
        {
            "Id": dataset_id,
            "source": {"type": invalid_source_type},
            "accessRights": "public",
        }
    )
    requests_mock.register_uri("GET", url, text=response, status_code=200)

    with pytest.raises(InvalidSourceTypeError):
        get_and_validate_dataset(dataset_id)


def test_get_and_validate_dataset_custom_source_type(requests_mock):
    dataset_id = "my-dataset"
    url = f"https://api.data-dev.oslo.systems/metadata/datasets/{dataset_id}"
    response = json.dumps(
        {"Id": dataset_id, "source": {"type": "event"}, "accessRights": "public"}
    )
    requests_mock.register_uri("GET", url, text=response, status_code=200)

    assert get_and_validate_dataset(dataset_id, "event") == {
        "Id": dataset_id,
        "source": {"type": "event"},
        "accessRights": "public",
    }


def test_get_and_validate_dataset_not_found(requests_mock):
    dataset_id = "my-dataset"
    url = f"https://api.data-dev.oslo.systems/metadata/datasets/{dataset_id}"
    response = json.dumps({"message": "Not found"})
    requests_mock.register_uri("GET", url, text=response, status_code=404)

    with pytest.raises(DatasetNotFoundError):
        get_and_validate_dataset(dataset_id)
