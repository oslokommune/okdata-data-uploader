import os
import json
import pytest

from generate_signed_post import handler, error_response, generate_signed_post


def setup_module():
    os.environ["BUCKET"] = "feh"


@pytest.fixture
def api_gateway_event():
    """
    Mock API Gateway event factory
    """

    def _event(
        principalId="test-dataplatform",
        body=json.dumps(
            {
                "datasetId": "foo",
                "versionId": "1",
                "editionId": "bar",
                "filename": "datastuff.txt",
            }
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
                "authorizer": {"principalId": f"{principalId}"},
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
        "body": json.dumps({"message": "lol"}),
    }


def test_handler_404_when_not_authenticated(api_gateway_event):
    event = api_gateway_event(principalId="fakeId")
    ret = handler(event, None)
    assert ret["statusCode"] == 403
    assert (
        json.loads(ret["body"])["message"]
        == "Forbidden. Only the test user can do this"
    )


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


def test_handler(api_gateway_event):
    event = api_gateway_event()
    ret = handler(event, None)
    assert ret["statusCode"] == 200
