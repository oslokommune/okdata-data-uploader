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

 - Connect to metadata API
 - Format/lint python?
 - Exclude more stuff from the artifact; it looks like botos dependencies are included, even though there's no reason to do that.
 - Logging
