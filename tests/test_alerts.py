import os
from unittest.mock import patch

import pytest

from uploader.alerts import _send_email, alert_if_new_columns
from uploader.errors import AlertEmailError


@patch("uploader.alerts.get_secret")
def test_send_email(get_secret, requests_mock):
    get_secret.return_value = "mega-secret"
    requests_mock.register_uri(
        "POST", os.environ["EMAIL_API_URL"], text="ok", status_code=200
    )
    res = _send_email(["foo@example.org"], "Test")
    assert res.status_code == 200
    assert res.text == "ok"


@patch("uploader.alerts.get_secret")
def test_send_email_error(get_secret, requests_mock):
    get_secret.return_value = "mega-secret"
    requests_mock.register_uri("POST", os.environ["EMAIL_API_URL"], status_code=500)
    with pytest.raises(AlertEmailError) as err:
        _send_email(["foo@example.org", "bar@example.org"], "Test")

    assert str(err.value).startswith(
        "Could not alert foo@example.org, bar@example.org:"
    )


@patch("uploader.alerts._send_email")
def test_alert_if_new_columns_no_new_columns(send_email, dataset, dynamodb):
    alert_if_new_columns(dataset["Id"], set())

    send_email.assert_not_called()


@patch("uploader.alerts._send_email")
def test_alert_if_new_columns_no_subscribers(send_email, dataset, dynamodb):
    table = dynamodb.Table("dataset-subscriptions")
    table.delete_item(Key={"DatasetId": dataset["Id"]})

    alert_if_new_columns(dataset["Id"], {"new_column"})

    send_email.assert_not_called()


@patch("uploader.alerts._send_email")
def test_alert_if_new_columns_single_new_column(send_email, dataset, dynamodb):
    alert_if_new_columns(dataset["Id"], {"new_column"})

    send_email.assert_called_once_with(
        ["test@example.org"],
        "En ny kolonne har blitt lagt til datasettet 'test-dataset':\n- new_column",
    )


@patch("uploader.alerts._send_email")
def test_alert_if_new_columns_multiple_new_columns(send_email, dataset, dynamodb):
    alert_if_new_columns(dataset["Id"], {"b_col", "a_col"})

    send_email.assert_called_once_with(
        ["test@example.org"],
        "Nye kolonner har blitt lagt til datasettet 'test-dataset':\n- a_col\n- b_col",
    )
