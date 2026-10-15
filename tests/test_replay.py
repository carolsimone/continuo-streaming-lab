from lab.replay import parse_args, summarize


def test_summarize_full_range_per_partition():
    s = summarize({0: list(range(5)), 1: list(range(3))}, {0: 5, 1: 3}, seconds=1.234)
    assert s["consumed"] == 8
    assert s["seconds"] == 1.23
    assert s["partitions"]["0"] == {"consumed": 5, "first_offset": 0, "last_offset": 4, "high_watermark": 5}
    assert s["partitions"]["1"]["last_offset"] == 2


def test_summarize_empty_partition_has_null_offsets():
    s = summarize({0: []}, {0: 0}, seconds=0.0)
    assert s["consumed"] == 0
    assert (s["partitions"]["0"]["first_offset"], s["partitions"]["0"]["last_offset"]) == (None, None)


def test_parse_args_defaults():
    args = parse_args([])
    assert args.bootstrap == "localhost:9092,localhost:9192,localhost:9292"
    assert args.registry == "http://localhost:8081"
    assert args.topic == "events"
    assert args.timeout == 120.0


def test_parse_args_defaults_follow_env(monkeypatch):
    monkeypatch.setenv("SCHEMA_REGISTRY_PORT", "18081")
    monkeypatch.setenv("KAFKA_1_PORT", "19092")
    args = parse_args([])
    assert args.registry == "http://localhost:18081"
    assert args.bootstrap.startswith("localhost:19092,")
