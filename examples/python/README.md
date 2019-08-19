Example on how to use the Origo metadata and data-uploader APIs

upload.py covers all steps needed from login, creation of versions, editions and the final upload to S3

# Install
```
pipenv install
```

Note: using pipenv for now to set up the environment as easy as possible for 3rd party developer,
we can change this later

Copy config-dist.ini to config.ini and update with your credentials

# Run
```
pipenv shell
python upload.py
```
