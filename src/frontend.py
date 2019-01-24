import os

_script = None
_content = None
_css = None


def handler(event, context):
    api_url = os.environ.get("API_URL", None)

    if not api_url:
        return {
            "isBase64Encoded": False,
            "statusCode": 500,
            "headers": {"Content-Type": "text/plain"},
            "body": "Nope",
        }

    return {
        "isBase64Encoded": False,
        "statusCode": 200,
        "headers": {"Content-Type": "text/html"},
        "body": get_html(api_url),
    }


def get_html(api_url):
    script = get_script()
    content = get_content()
    css = get_css()

    return (
        "<!DOCTYPE html>"
        "<html>"
        "<head>"
        '<meta charset="utf-8">'
        f'<style type="text/css">{css}</style>'
        "</head>"
        f"<body>{content}"
        f"<script>window._serviceEndpoint = '{api_url}';</script>"
        f"<script>{script}</script>"
        "</body>"
        "</html>"
    )


def get_script():
    global _script
    if not _script:
        with open("src/frontend.js") as f:
            _script = f.read()

    return _script


def get_content():
    global _content
    if not _content:
        with open("src/frontend.html") as f:
            _content = f.read()

    return _content


def get_css():
    global _css
    if not _css:
        with open("src/frontend.css") as f:
            _css = f.read()
    return _css


if __name__ == "__main__":
    print("Startint test server on http://localhost:8000")

    from http.server import HTTPServer, BaseHTTPRequestHandler

    class HttpTest(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(get_html("").encode())

    httpd = HTTPServer(("localhost", 8000), HttpTest)
    httpd.serve_forever()
