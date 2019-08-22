import os
import requests
import logging
from json.decoder import JSONDecodeError

log = logging.getLogger()
log.setLevel(logging.INFO)

AUTHORIZER_API = os.environ["AUTHORIZER_API"]


def is_owner(authorization_header, dataset_id):
    r = requests.get(
        f"{AUTHORIZER_API}/{dataset_id}",
        headers={"Authorization": authorization_header},
    )

    try:
        data = r.json()
        return "access" in data and data["access"]
    except JSONDecodeError as e:
        log.exception(f"Authorization JSON decode failure: {e}")

    return False
