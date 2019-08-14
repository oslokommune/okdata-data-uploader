from configparser import ConfigParser
from sdk.data_uploader import DataUploader

config = ConfigParser()
config.read('config.ini')

#####
# Datasets to be added to metadata API
datasetData = {
    "title": "H.Eide test2",
    "description": "H.Eide test data",
    "keywords": ["eide", "test"],
    "frequency": "hourly",
    "accessRights": ":non-public",
    "privacyLevel": "green",
    "objective": "Formålsbeskrivelse",
    "contactPoint": {
        "name": "***REMOVED***",
        "email": "***REMOVED***",
        "phone": "***REMOVED***"
    },
    "publisher": "EIDE"
}
datasetVersionData = {
    "version": "6",
    "schema": {},
    "transformation": {}
}
datasetVersionEditionData = {
    "edition": "2019-05-28T15:37:00+02:00",
    "description": "Data for one hour",
    "startTime": "2018-12-21T08:00:00+01:00",
    "endTime": "2018-12-21T09:00:00+01:00"
}

######
# The dataset* variables are optional, if these are set in config.ini this script will
# not run the relevant DataUploader function
datasetId = config.get("dataUploader", "datasetId", fallback=None)
datasetVersion = config.get("dataUploader", "datasetVersion", fallback=None)
datasetVersionEdition = config.get("dataUploader", "datasetVersionEdition", fallback=None)

upload = DataUploader(config)
try:
    print("Uploading a file to S3")
    upload.login()
    if (datasetId == None):
        upload.createDataset(datasetData)
    if (datasetVersion == None):
        upload.createVersion(datasetVersionData)
    if (datasetVersionEdition == None):
        upload.createEdition(datasetVersionEditionData)
    if (upload.upload("README.md")):
        print("Done... go brew some coffee")
    else:
        print("Could not upload file....")
except Exception as e:
    print(">> Something went horrible wrong")
    print(e)

# To upload with curl: cmd = upload.curl("tmp3.zip")
#   Max upload size for now is 5GB
