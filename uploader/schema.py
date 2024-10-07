import json


def get_model_schema(name):
    with open(f"doc/models/{name}.json") as f:
        return json.loads(f.read())
