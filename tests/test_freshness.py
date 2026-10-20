import json

import pytest

from lab import freshness
from lab.freshness import elapsed_since, parse_args


def test_elapsed_since_rounds_to_two_decimals():
    assert elapsed_since(10.0, 12.3456) == 2.35


def test_elapsed_since_rejects_negative_interval():
    with pytest.raises(ValueError):
        elapsed_since(5.0, 4.0)


def test_parse_args_defaults():
    args = parse_args([])
    assert (args.segment_ms, args.poll, args.timeout) == (10000, 2.0, 300.0)
    assert args.filler_interval == 1.0
    assert args.bootstrap == "localhost:9092,localhost:9192,localhost:9292"
    assert args.rest == "http://localhost:8181"


def test_parse_args_no_filler():
    assert parse_args(["--filler-interval", "0"]).filler_interval == 0.0


def test_tick_follows_filler_interval_or_poll():
    assert freshness.tick_seconds(parse_args([])) == 1.0
    assert freshness.tick_seconds(parse_args(["--filler-interval", "0", "--poll", "3"])) == 3.0


def test_main_timeout_exits_1_and_keeps_stdout_empty(monkeypatch, capsys):
    def boom(_args):
        raise TimeoutError("not queryable")

    monkeypatch.setattr(freshness, "measure", boom)
    assert freshness.main([]) == 1
    assert capsys.readouterr().out == ""


def test_main_prints_one_json_line(monkeypatch, capsys):
    result = {"seconds_to_queryable": 12.5, "segment_ms": 5000}
    monkeypatch.setattr(freshness, "measure", lambda _args: result)
    assert freshness.main([]) == 0
    assert json.loads(capsys.readouterr().out) == result
