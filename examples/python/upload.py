import logging
from configparser import ConfigParser
from sdk.data_uploader import DataUploader

logging.basicConfig(level=logging.INFO)
log = logging.getLogger()

config = ConfigParser()
config.read("config.ini")

#####
# Datasets to be added to metadata API
datasetData = {
    "title": "Test",
    "description": "Test data",
    "keywords": ["test"],
    "accessRights": "non-public",
    "objective": "Formålsbeskrivelse",
    "contactPoint": {
        "name": "Tim",
        "email": "tim@example.org",
        "phone": "12345678",
    },
    "publisher": "Tim",
}
datasetVersionData = {"version": "6", "schema": {}, "transformation": {}}
datasetVersionEditionData = {
    "edition": "2019-05-28T15:37:00+02:00",
    "description": "Data for one hour",
    "startTime": "2018-12-21T08:00:00+01:00",
    "endTime": "2018-12-21T09:00:00+01:00",
}

######
# The dataset* variables are optional, if these are set in config.ini this script will
# not run the relevant DataUploader function
datasetId = config.get("dataUploader", "datasetId", fallback=None)
datasetVersion = config.get("dataUploader", "datasetVersion", fallback=None)
datasetVersionEdition = config.get(
    "dataUploader", "datasetVersionEdition", fallback=None
)

upload = DataUploader(config)
try:
    log.info("Uploading a file to S3")
    upload.login()
    if datasetId is None:
        upload.createDataset(datasetData)
    if datasetVersion is None:
        upload.createVersion(datasetVersionData)
    if datasetVersionEdition is None:
        upload.createEdition(datasetVersionEditionData)

    log.info(f"Dataset: {upload.datasetId}")
    log.info(f"Version: {upload.datasetVersion}")
    log.info(f"Edition: {upload.datasetVersionEdition}")

    if upload.upload("README.md"):
        log.info("Done... go brew some coffee")
    else:
        log.error("Could not upload file....")
except Exception as e:
    log.exception(f">> Something went horrible wrong:\n{e}")

# To upload with curl: cmd = upload.curl("tmp3.zip")
#   Max upload size for now is 5GB
