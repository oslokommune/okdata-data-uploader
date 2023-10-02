# Data uploader

REST API for creating presigned URLs and form fields that can be used to POST a file to S3.

## Setup

1. [Install Serverless Framework](https://serverless.com/framework/docs/getting-started/)
2. Install plugins:
```
make init
```

### Setup for development

Grab yourself a virtualenv:
```
python -m venv .venv
```

To activate the virtualenv run:
```
source ./venv/bin/activate
```

Inside the virtualenv you can install packages locally:
```
pip install -r requirements.txt
```

To exit from a virtualenv run:
```
deactivate
```

## Running tests

```
$ make test
```

Tests are run using [tox](https://pypi.org/project/tox/).

## Deploy

Deploy to both dev and prod is automatic via GitHub Actions on push to
`main`. You can alternatively deploy from local machine with: `make deploy` or
`make deploy-prod`.

## Code formatting

```
make format
```

Runs [black](https://black.readthedocs.io/en/stable/) to format python code.

## Upload size

A single PUT can be up to 5GB for S3 signed URLs (and is then our current limitation), over that and a multi-part upload must be created

## TODO

 - Revisit the upload flow
   - Today: frontend checks dataset/schema and creates edition, then POSTs file
   - Alternative: frontend POSTs filename/metadata, backend checks dataset/schema
     - Alt 1: return signed s3 url, frontend POSTs, (new) backend waits for S3 event and then creates edition (where should metadata be stored in the meantime?)
     - Alt 2: create edition, return s3 url
 - Create script to get signed URLs for multipart, and script to upload these parts and combine them
   - https://github.com/sandyghai/AWS-S3-Multipart-Upload-Using-Presigned-Url
