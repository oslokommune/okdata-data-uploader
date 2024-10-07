import re
import json

import pytest
from freezegun import freeze_time
from okdata.resource_auth import ResourceAuthorizer

from uploader.common import error_response, split_edition_id
from uploader.errors import InvalidDatasetEditionError
from uploader.handlers.generate_signed_post import (
    ENABLE_AUTH,
    handler,
)


@pytest.fixture(autouse=True)
def authorizer(monkeypatch):
    def check_token(self, token, scope, resource_name):
        return (
            token == "Bjørnepollett"
            and scope == "okdata:dataset:write"
            and resource_name.startswith("okdata:dataset:")
        )

    monkeypatch.setattr(ResourceAuthorizer, "has_access", check_token)


@pytest.fixture
def api_gateway_event():
    """
    Mock API Gateway event factory
    """

    def _event(
        authorization_header="Bearer Bjørnepollett",
        body=json.dumps(
            {"editionId": "datasetid/1/20190101T125959", "filename": "datastuff.txt"}
        ),
    ):
        return {
            "body": body,
            "httpMethod": "POST",
            "isBase64Encoded": True,
            "queryStringParameters": {},
            "pathParameters": {},
            "stageVariables": {},
            "stage": "dev",
            "headers": {
                "Authorization": authorization_header,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Encoding": "gzip, deflate, sdch",
                "Accept-Language": "en-US,en;q=0.8",
                "Cache-Control": "max-age=0",
                "CloudFront-Forwarded-Proto": "https",
                "CloudFront-Is-Desktop-Viewer": "true",
                "CloudFront-Is-Mobile-Viewer": "false",
                "CloudFront-Is-SmartTV-Viewer": "false",
                "CloudFront-Is-Tablet-Viewer": "false",
                "CloudFront-Viewer-Country": "US",
                "Host": "1234567890.execute-api.eu-central-1.amazonaws.com",
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": "Custom User Agent String",
                "Via": "1.1 08f323deadbeefa7af34d5feb414ce27.cloudfront.net (CloudFront)",
                "X-Amz-Cf-Id": "cDehVQoZnx43VYQb9j2-nvCh-9z396Uhbp027Y2JvkCPNLmGJHqlaA==",
                "X-Forwarded-For": "127.0.0.1, 127.0.0.2",
                "X-Forwarded-Port": "443",
                "X-Forwarded-Proto": "https",
            },
            "requestContext": {
                "accountId": "123456789012",
                "resourceId": "123456",
                "stage": "dev",
                "requestId": "c6af9ac6-7b61-11e6-9a41-93e8deadbeef",
                "requestTime": "09/Apr/2015:12:34:56 +0000",
                "requestTimeEpoch": 1_428_582_896_000,
                "identity": {
                    "cognitoIdentityPoolId": None,
                    "accountId": None,
                    "cognitoIdentityId": None,
                    "caller": None,
                    "accessKey": None,
                    "sourceIp": "127.0.0.1",
                    "cognitoAuthenticationType": None,
                    "cognitoAuthenticationProvider": None,
                    "userArn": None,
                    "userAgent": "Custom User Agent String",
                    "user": None,
                },
                "authorizer": {"principalId": "abc123456"},
                "httpMethod": "POST",
                "apiId": "1234567890",
                "protocol": "HTTP/1.1",
            },
        }

    return _event


def test_error_response():
    assert error_response(123, "lol") == {
        "isBase64Encoded": False,
        "statusCode": 123,
        "headers": {"Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"message": "lol"}),
    }


def test_split_edition_id():
    with pytest.raises(InvalidDatasetEditionError):
        split_edition_id("a/b")

    assert split_edition_id("a/b/c") == ("a", "b")

    with pytest.raises(InvalidDatasetEditionError):
        split_edition_id("fubar")


@pytest.mark.skipif(not ENABLE_AUTH, reason="Auth is disabled")
def test_handler_403_when_not_authenticated(api_gateway_event, requests_mock):
    url = "https://api.data-dev.oslo.systems/metadata/datasets/datasetid"
    response = json.dumps({"accessRights": "restricted", "source": {"type": "file"}})
    requests_mock.register_uri("GET", url, text=response, status_code=200)

    event = api_gateway_event(authorization_header="Snusk")
    ret = handler(event, None)

    assert ret["statusCode"] == 403
    assert json.loads(ret["body"])["message"] == "Forbidden"


def test_handler_bad_json(api_gateway_event):
    event = api_gateway_event(body='"} invalid json')
    ret = handler(event, None)
    assert ret["statusCode"] == 400
    assert json.loads(ret["body"])["message"] == "Body is not a valid JSON document"


def test_handler_invalid_json(api_gateway_event):
    event = api_gateway_event(body=json.dumps({}))
    ret = handler(event, None)
    assert ret["statusCode"] == 400
    assert (
        json.loads(ret["body"])["message"]
        == "JSON document does not conform to the given schema"
    )


def test_handler_invalid_edition_format(api_gateway_event):
    event = api_gateway_event(
        body=json.dumps({"editionId": "invalid", "filename": "foo.txt"})
    )
    res = handler(event, None)
    assert res["statusCode"] == 422


def test_handler_invalid_source_type(api_gateway_event, requests_mock):
    url = "https://api.data-dev.oslo.systems/metadata/datasets/datasetid"
    response = json.dumps(
        {"accessRights": "restricted", "source": {"type": "database"}}
    )
    requests_mock.register_uri("GET", url, text=response, status_code=200)

    event = api_gateway_event()
    response = handler(event, None)
    assert response["statusCode"] == 400
    assert json.loads(response["body"]) == {
        "message": "Invalid source.type 'database' for dataset: datasetid. Must be source.type='file'"
    }


@freeze_time("2020-11-02T19:54:14.123456+00:00")
def test_handler(api_gateway_event, requests_mock):
    url = "https://api.data-dev.oslo.systems/metadata/datasets/datasetid"
    response = json.dumps({"accessRights": "restricted", "source": {"type": "file"}})
    requests_mock.register_uri("GET", url, text=response, status_code=200)

    url = "https://api.data-dev.oslo.systems/metadata/datasets/datasetid/versions/1/editions/20190101T125959"
    response = json.dumps({"Id": "datasetid/1/20190101T125959"})
    requests_mock.register_uri("GET", url, text=response, status_code=200)

    url = "https://api.data-dev.oslo.systems/status-api/status/*"
    matcher = re.compile(url)
    trace_id = "my-dataset-00a1bcd2-e3f4-5a6b-c678-9c1defa234b5"
    m = requests_mock.register_uri(
        "POST", matcher, json={"trace_id": trace_id}, status_code=200
    )

    event = api_gateway_event()
    ret = handler(event, None)

    assert ret["statusCode"] == 200
    assert m.called_once
    assert m.last_request.json() == {
        "domain": "dataset",
        "domain_id": "datasetid/1",
        "component": "data-uploader",
        "operation": "upload",
        "user": "abc123456",
        "start_time": "2020-11-02T19:54:14.123456+00:00",
        "s3_path": "raw/yellow/datasetid/version=1/edition=20190101T125959/datastuff.txt",
        "end_time": "2020-11-02T19:54:14.123456+00:00",
    }

    response_body = json.loads(ret["body"])
    assert response_body["status_response"] == trace_id
    assert response_body["trace_id"] == trace_id


def test_handler_404_response(api_gateway_event, requests_mock):
    url = "https://api.data-dev.oslo.systems/metadata/datasets/datasetid"
    response = json.dumps({"message": "Not found"})
    requests_mock.register_uri("GET", url, text=response, status_code=404)

    event = api_gateway_event()
    ret = handler(event, None)
    assert ret["statusCode"] == 404
    assert json.loads(ret["body"]) == {"message": "Dataset datasetid does not exist"}


def test_s3_confidentiality_path_yellow(api_gateway_event, requests_mock):
    url = (
        "https://api.data-dev.oslo.systems/metadata/datasets/alder-distribusjon-status"
    )
    response = json.dumps({"accessRights": "restricted", "source": {"type": "file"}})
    requests_mock.register_uri("GET", url, text=response, status_code=200)

    url = "https://api.data-dev.oslo.systems/metadata/datasets/alder-distribusjon-status/versions/1/editions/20190101T125959"
    response = json.dumps({"Id": "alder-distribusjon-status/1/20190101T125959"})
    requests_mock.register_uri("GET", url, text=response, status_code=200)

    url = "https://api.data-dev.oslo.systems/status-api/status/*"
    matcher = re.compile(url)
    trace_id = "my-dataset-00a1bcd2-e3f4-5a6b-c678-9c1defa234b5"
    requests_mock.register_uri(
        "POST", matcher, json={"trace_id": trace_id}, status_code=200
    )

    event = api_gateway_event()
    postBody = json.loads(event["body"])
    postBody["editionId"] = "alder-distribusjon-status/1/20190101T125959"
    event["body"] = json.dumps(postBody)

    ret = handler(event, None)
    response_body = json.loads(ret["body"])
    key = response_body["fields"]["key"]
    assert "/yellow/" in key
    assert response_body["status_response"] == trace_id
    assert response_body["trace_id"] == trace_id


def test_s3_confidentiality_path_green(api_gateway_event, requests_mock):
    url = "https://api.data-dev.oslo.systems/metadata/datasets/badetemperatur"
    response = json.dumps({"accessRights": "public", "source": {"type": "file"}})
    requests_mock.register_uri("GET", url, text=response, status_code=200)

    url = "https://api.data-dev.oslo.systems/metadata/datasets/badetemperatur/versions/1/editions/20190101T125959"
    response = json.dumps({"Id": "badetemperatur/1/20190101T125959"})
    requests_mock.register_uri("GET", url, text=response, status_code=200)

    url = "https://api.data-dev.oslo.systems/status-api/status/*"
    matcher = re.compile(url)
    trace_id = "my-dataset-00a1bcd2-e3f4-5a6b-c678-9c1defa234b5"
    requests_mock.register_uri(
        "POST", matcher, json={"trace_id": trace_id}, status_code=200
    )

    event = api_gateway_event()
    postBody = json.loads(event["body"])
    postBody["editionId"] = "badetemperatur/1/20190101T125959"
    event["body"] = json.dumps(postBody)

    ret = handler(event, None)
    response_body = json.loads(ret["body"])
    key = response_body["fields"]["key"]
    assert "/green/" in key
    assert response_body["status_response"] == trace_id
    assert response_body["trace_id"] == trace_id


def test_s3_confidentiality_path_no_access_rights_response(
    api_gateway_event, requests_mock
):
    url = "https://api.data-dev.oslo.systems/metadata/datasets/badetemperatur"
    response = json.dumps({"hello": "world", "source": {"type": "file"}})
    requests_mock.register_uri("GET", url, text=response, status_code=200)

    url = "https://api.data-dev.oslo.systems/metadata/datasets/badetemperatur/versions/1/editions/20190101T125959"
    response = json.dumps({"Id": "badetemperatur/1/20190101T125959"})
    requests_mock.register_uri("GET", url, text=response, status_code=200)

    url = "https://api.data-dev.oslo.systems/status-api/status/*"
    matcher = re.compile(url)
    trace_id = "my-dataset-00a1bcd2-e3f4-5a6b-c678-9c1defa234b5"
    requests_mock.register_uri(
        "POST", matcher, json={"trace_id": trace_id}, status_code=200
    )

    event = api_gateway_event()
    postBody = json.loads(event["body"])
    postBody["editionId"] = "badetemperatur/1/20190101T125959"
    event["body"] = json.dumps(postBody)

    ret = handler(event, None)
    assert ret["statusCode"] == 400
