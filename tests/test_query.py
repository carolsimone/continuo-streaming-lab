import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from lab.query import main, parse_args, render_sql


def test_render_sql_replaces_table_placeholder():
    sql = render_sql("SELECT count(*) FROM {table}", "s3://warehouse/x/metadata/v1.metadata.json")
    assert sql == "SELECT count(*) FROM iceberg_scan('s3://warehouse/x/metadata/v1.metadata.json')"


def test_missing_table_exits_3_with_one_line_explanation(capsys):
    class NotFound(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(404)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), NotFound)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        code = main(["--rest", f"http://127.0.0.1:{server.server_port}"])
    finally:
        server.shutdown()
    captured = capsys.readouterr()
    assert code == 3
    assert captured.out == ""
    assert "no segment has been offloaded yet" in captured.err


def test_parse_args_defaults():
    args = parse_args([])
    assert args.rest == "http://localhost:8181"
    assert args.minio == "localhost:9000"


def test_parse_args_defaults_follow_env_at_parse_time(monkeypatch):
    monkeypatch.setenv("ICEBERG_REST_PORT", "18181")
    monkeypatch.setenv("MINIO_API_PORT", "19000")
    args = parse_args([])
    assert args.rest == "http://localhost:18181"
    assert args.minio == "localhost:19000"
