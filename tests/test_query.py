import logging
import os
import shutil
import stat
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import duckdb
import pytest

from lab.query import connect, main, parse_args, render_sql

REPO = Path(__file__).resolve().parent.parent


def _serve(handler):
    server = HTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def _stop(server):
    server.shutdown()
    server.server_close()


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

    server = _serve(NotFound)
    try:
        code = main(["--rest", f"http://127.0.0.1:{server.server_port}"])
    finally:
        _stop(server)
    captured = capsys.readouterr()
    assert code == 3
    assert captured.out == ""
    assert "no segment has been offloaded yet" in captured.err
    assert len(captured.err.strip().splitlines()) == 1


def test_render_sql_escapes_single_quote_in_location():
    assert render_sql("SELECT * FROM {table}", "s3://b/it's/m.json") == "SELECT * FROM iceberg_scan('s3://b/it''s/m.json')"


def test_unreachable_catalog_exits_1_and_says_so(capsys):
    server = _serve(BaseHTTPRequestHandler)
    port = server.server_port
    _stop(server)  # the port is now closed
    code = main(["--rest", f"http://127.0.0.1:{port}"])
    captured = capsys.readouterr()
    assert code == 1
    assert captured.out == ""
    assert "cannot reach the catalog at" in captured.err


def test_unexpected_catalog_response_exits_1_and_says_so(capsys):
    class Empty(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"{}")

        def log_message(self, *args):
            pass

    server = _serve(Empty)
    try:
        code = main(["--rest", f"http://127.0.0.1:{server.server_port}"])
    finally:
        _stop(server)
    captured = capsys.readouterr()
    assert code == 1
    assert captured.out == ""
    assert "unexpected catalog response" in captured.err
    assert "cannot reach" not in captured.err


def test_connect_hints_at_one_time_online_install_when_extension_install_fails(monkeypatch, caplog):
    class Con:
        def execute(self, sql):
            raise duckdb.IOException("HTTP Error: could not download extension")

    monkeypatch.setattr(duckdb, "connect", lambda: Con())
    with caplog.at_level(logging.ERROR), pytest.raises(duckdb.IOException):
        connect("localhost:9000")
    assert "one-time online install" in caplog.text


@pytest.mark.skipif(shutil.which("make") is None, reason="make is not installed")
class TestMakeQuery:
    """`make query` must hand SQL to the tool byte for byte (quotes, spaces, $, backticks)."""

    # What the tool must receive. On the make command line a literal $ has to be written $$ (make's own rule).
    SQL = "SELECT 1 AS \"my col\", '$x' AS d, `echo hi` AS b FROM {table} WHERE a = $y"

    @pytest.fixture
    def env(self, tmp_path):
        fake_uv = tmp_path / "uv"  # prints each argument as [arg] instead of running the tool
        fake_uv.write_text('#!/bin/sh\nfor a in "$@"; do printf "[%s]\\n" "$a"; done\n')
        fake_uv.chmod(fake_uv.stat().st_mode | stat.S_IEXEC)
        env = {**os.environ, "PATH": f"{tmp_path}{os.pathsep}{os.environ['PATH']}"}
        env.pop("SQL", None)
        return env

    def _make(self, env, *args):
        return subprocess.run(["make", *args, "query"], cwd=REPO, env=env, capture_output=True, text=True, check=True).stdout

    def test_command_line_sql_survives_intact(self, env):
        out = self._make(env, "-s", "SQL=" + self.SQL.replace("$", "$$"))
        assert out.splitlines()[-2:] == ["[--sql]", f"[{self.SQL}]"]

    def test_sql_from_the_shell_environment_survives_intact(self, env):
        out = self._make({**env, "SQL": self.SQL}, "-s")
        assert out.splitlines()[-2:] == ["[--sql]", f"[{self.SQL}]"]

    def test_sql_is_never_inlined_into_the_command(self, env):
        out = self._make(env, "-n", "SQL=" + self.SQL.replace("$", "$$"))
        assert "my col" not in out
        assert '"$SQL"' in out

    def test_unset_sql_runs_the_default_query(self, env):
        out = self._make(env, "-s")
        assert "--sql" not in out
