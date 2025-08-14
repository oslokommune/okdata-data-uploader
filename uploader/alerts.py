import os

import boto3
import requests
from okdata.aws.ssm import get_secret


def _send_email(to_emails, body):
    res = requests.post(
        os.environ["EMAIL_API_URL"],
        json={
            "mottakerepost": to_emails,
            "avsenderepost": "dataplattform@oslo.kommune.no",
            "avsendernavn": "Dataspeilet",
            "emne": "Endring i datastruktur",
            "meldingskropp": body.replace("\n", "<br />"),
        },
        headers={"apikey": get_secret("/dataplatform/shared/email-api-key")},
    )
    res.raise_for_status()
    return res


def alert_if_new_columns(dataset_id, new_columns):
    if not new_columns:
        return

    dynamodb = boto3.resource("dynamodb", region_name=os.environ["AWS_REGION"])
    subscriptions_table = dynamodb.Table("dataset-subscriptions")

    subscriptions_entry = subscriptions_table.get_item(Key={"DatasetId": dataset_id})

    if item := subscriptions_entry.get("Item"):
        multi = len(new_columns) > 1

        text = "{} har blitt lagt til datasettet '{}':\n{}".format(
            "Nye kolonner" if multi else "En ny kolonne",
            dataset_id,
            "\n".join(f"- {c}" for c in sorted(new_columns)),
        )

        _send_email(item["Subscribers"], text)
