# Data uploader

Returns a presigned URL and form fields that can be used to POST a file to S3.

## Deploy

With `npm` and `serverless` installed:

`make deploy`

## Code fromatting

Run `make fmt`

 - Uses `prettier` to format json and yaml files.
 - Uses `black` to format python.

## TODO

 - Revisit the upload flow
  - Today: frontend checks dataset/schema and creates edition, then POSTs file
  - Alternative: frontend POSTs filename/metadata, backend checks dataset/schema
   - Alt 1: return signed s3 url, frontend POSTs, (new) backend waits for S3 event and then creates edition (where should metadata be stored in the meantime?)
   - Alt 2: create edition, return s3 url
