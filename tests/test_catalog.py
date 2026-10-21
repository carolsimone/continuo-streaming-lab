import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from lab.catalog import TableNotFound, table_metadata_location


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/v1/namespaces/default/tables/events":
            body = json.dumps({"metadata-location": "s3://warehouse/default/events/metadata/v3.metadata.json"}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *args):  # keep test output quiet
        pass


@pytest.fixture
def rest_url():
    server = HTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_port}"
    server.shutdown()
    server.server_close()


def test_returns_metadata_location(rest_url):
    loc = table_metadata_location(rest_url, "default", "events")
    assert loc == "s3://warehouse/default/events/metadata/v3.metadata.json"


def test_missing_table_raises_table_not_found(rest_url):
    with pytest.raises(TableNotFound, match="default.orders"):
        table_metadata_location(rest_url, "default", "orders")


def test_trailing_slash_and_special_characters_are_handled():
    seen = []

    class Recorder(BaseHTTPRequestHandler):
        def do_GET(self):
            seen.append(self.path)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"metadata-location": "s3://w/m.json"}')

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Recorder)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        loc = table_metadata_location(f"http://127.0.0.1:{server.server_port}/", "a b", "t/x?y")
    finally:
        server.shutdown()
        server.server_close()
    assert loc == "s3://w/m.json"
    assert seen == ["/v1/namespaces/a%20b/tables/t%2Fx%3Fy"]
