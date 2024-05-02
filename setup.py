import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()


setuptools.setup(
    name="okdata-data-uploader",
    version="0.0.1",
    author="Origo Dataplattform",
    author_email="dataplattform@oslo.kommune.no",
    description="Returns a presigned URL and form fields that can be used to POST a file to S3",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/oslokommune/okdata-data-uploader",
    packages=setuptools.find_packages(),
    install_requires=[
        "boto3",
        "aws-xray-sdk",
        "jsonschema",
        "requests",
        "okdata-aws>=4.1",
        "okdata-resource-auth",
    ],
)
