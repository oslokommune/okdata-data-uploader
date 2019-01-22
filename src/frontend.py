import os

def handler(event, context):
    api_url = os.environ["API_URL"]

    if not api_url:
        return {
            "isBase64Encoded": False,
            "statusCode": 500,
            "headers": {
                "Content-Type": "text/plain",
            },
            "body": "Nope",
        }

    return {
        "isBase64Encoded": False,
        "statusCode": 200,
        "headers": {
            "Content-Type": "text/html",
        },
        "body": get_html(api_url),
    }

def get_html(api_url):
    return (
        '<!DOCTYPE html>'
        '<html>'
        '<head>'
        '    <meta charset="utf-8">'
        '</head>'
        '<body>'
        '    <h1>Upload the data</h1>'
        '    <div class="error">'
        '    </div>'
        '    <div>'
        '    <label>Bearer token <input type="text" id="bearerToken"></label>'
        '    </div>'
        '    <div>'
        '    <label>Distribution <input type="text" id="distributionId"></label>'
        '    </div>'
        '    <div>'
        '    <label>Select file <input type="file" id="file"></label>'
        '    </div>'
        '    <div>'
        '    <button type="submit" id="upload">Upload</button>'
        '    </div>'
        '    <script>'
        '        (function () {'
        f'        const serviceEndpoint = "{api_url}";'
        '        const $distributionId = document.querySelector("#distributionId");'
        '        const $bearerToken = document.querySelector("#bearerToken");'
        '        const $file = document.querySelector("#file");'
        '        const $upload = document.querySelector("#upload");'
        '        const $error = document.querySelector(".error");'
        '        function createDistribution(bearerToken, distributionId) {'
        '            return fetch(serviceEndpoint, {'
        '            method: "POST",'
        '            headers: new Headers({'
        '                "Authorization": `Bearer ${bearerToken}`'
        '            }),'
        '            body: JSON.stringify({distributionId}),'
        '            })'
        '        }'
        '        function postToS3(url, fields, file) {'
        '            const formData = new FormData();'
        '            for (const field in fields) {'
        '            if (fields.hasOwnProperty(field)) {'
        '                formData.append(field, fields[field]);'
        '            }'
        '            }'
        '            formData.append("file", file);'
        '            console.log("POST TO S3", url, formData);'
        '            return fetch(url, {'
        '            method: "POST",'
        '            body: formData,'
        '            });'
        '        }'
        '        function error(msg) {'
        '            console.error(msg);'
        '            $error.textContent = msg;'
        '        }'
        '        function upload() {'
        '            const distributionId = $distributionId.value;'
        '            const bearerToken = $bearerToken.value;'
        '            const files = $file.files;'

        '            if (files.length !== 1) {'
        '            error("Select 1 file");'
        '            return;'
        '            }'
        '            const file = files[0];'
        '            createDistribution(bearerToken, distributionId)'
        '            .then((res) => res.json())'
        '            .then((data) => postToS3(data.url, data.fields, file))'
        '            .then(() => error("Uploaded?"))'
        '            .catch((e) => error(e));'
        '        }'
        '        $upload.addEventListener("click", upload);'
        '        })();'
        '    </script>'
        '</body>'
        '</html>'
    )
