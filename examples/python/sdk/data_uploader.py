import requests
import json

class DataUploader():
    def __init__(self, config):
        self.config = config
        self.configKey = "dataUploader"
        self.accessToken = None
        self.datasetId = self.getConfig("datasetId", None)
        self.datasetVersion = self.getConfig("datasetVersion", None)
        self.datasetVersionEdition = self.getConfig("datasetVersionEdition", None)
        baseUrl = self.getConfig("datasetsUrl")
        if (baseUrl == None):
            raise Exception("No datasetsUrl set in config")

    def login(self):
        data = {
            "client_id": self.getConfig("clientId"),
            "client_secret": self.getConfig("clientSecret"),
            "grant_type": "client_credentials"
        }
        url = self.getConfig("loginUrl")
        loginResult = requests.post(url, data=data).json()
        if ("error" in loginResult):
            description = loginResult["error_description"]
            raise Exception(f"Could not authenticate client: {description}")

        self.accessToken = loginResult["access_token"]
        return self.accessToken

    def createDataset(self, data):
        url = self.getConfig("datasetsUrl")
        self.datasetId = requests.post(url, data=json.dumps(data)).text
        return self.datasetId

    def createVersion(self, data):
        baseUrl = self.getConfig("datasetsUrl")
        url = f"{baseUrl}/{self.datasetId}/versions"
        result = requests.post(url, data=json.dumps(data))
        if (result.status_code == 409):
            version = data["version"]
            raise Exception(f"Version: {version} on datasetId {self.datasetId} already exists")

        resultText = result.text.replace('"', '')
        self.datasetVersion = resultText.split("/")[1]
        return self.datasetVersion

    def createEdition(self, data):
        if (self.datasetVersion == None):
            raise Exception("No Dataset Version set")

        baseUrl = self.getConfig("datasetsUrl")
        url = f"{baseUrl}/{self.datasetId}/versions/{self.datasetVersion}/editions"
        result = requests.post(url, data=json.dumps(data))
        if (result.status_code == 409):
            edition = data["edition"]
            raise Exception(f"Edition: {edition} on datasetId {self.datasetId} already exists")

        resultText = result.text.replace('"', '')
        self.datasetVersionEdition = resultText
        return self.datasetVersionEdition

    def curl(self, fileName):
        url = self.getConfig("s3BucketUrl")
        s3SignedData = self.createS3SignedData(fileName)
        str = 'curl ';
        for var in s3SignedData["fields"]:
            varValue = s3SignedData["fields"][var]
            val = f"{var}={varValue}"
            str = f"{str} -F \"{val}\" "
            str = f"{str} -F \"file=@{fileName}\""
            str = f"{str} {url}"
        return str

    def upload(self, fileName):
        url = self.getConfig("s3BucketUrl")
        if (url == None):
            raise Exception("No s3 Bucket URL set")

        s3SignedData = self.createS3SignedData(fileName)
        s3Data = {}
        for var in s3SignedData["fields"]:
            s3Data[var] = s3SignedData["fields"][var]

        files = {
            'file': open(fileName, 'rb')
        }
        result = requests.post(url, data=s3Data, files=files)
        return result.status_code == 204

    def createS3SignedData(self, fileName):
        if (self.datasetVersionEdition == None):
            raise Exception("Version Edition not set")

        data = {
            "filename": fileName,
            "editionId": self.datasetVersionEdition
        }
        headers = {
            'Authorization': f"Bearer {self.accessToken}"
        }
        url = self.getConfig("signedS3Url")
        if (url == None):
            raise Exception("No Signed S3 URL set")

        result = requests.post(url, data=json.dumps(data), headers=headers)
        return result.json()

    def getConfig(self, key, fallback=None):
        return self.config.get(self.configKey, key, fallback=fallback)

    def debugDump(self):
        print(self.config)
