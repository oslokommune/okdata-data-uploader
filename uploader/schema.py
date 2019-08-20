import json

request_schema = None

with open("doc/models/uploadRequest.json") as f:
    request_schema = json.loads(f.read())
