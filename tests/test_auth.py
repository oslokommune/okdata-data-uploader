import re
import pytest
from uploader.auth import is_owner


@pytest.fixture(params=[{"access": False}, {"flarpa": "blarpa"}])
def auth_request_false(request, requests_mock):
    matcher = re.compile("https://example.com/.*")
    requests_mock.register_uri("GET", matcher, json=request.param)


@pytest.fixture()
def auth_request_true(requests_mock):
    matcher = re.compile("https://example.com/.*")
    requests_mock.register_uri("GET", matcher, json={"access": True})


@pytest.fixture(params=[400, 403, 500])
def auth_request_not_200(request, requests_mock):
    matcher = re.compile("https://example.com/.*")
    requests_mock.register_uri("GET", matcher, status_code=request.param)


@pytest.fixture()
def auth_request_not_json(request, requests_mock):
    matcher = re.compile("https://example.com/.*")
    requests_mock.register_uri("GET", matcher, text="not json")


def test_not_owner(auth_request_false):
    assert not is_owner("blabla123", "data-1")


def test_is_owner(auth_request_true):
    assert is_owner("blabla123", "data-1")


def test_not_200(auth_request_not_200):
    assert not is_owner("blabla123", "data-1")


def test_not_json(auth_request_not_json):
    assert not is_owner("blabla123", "data-1")
