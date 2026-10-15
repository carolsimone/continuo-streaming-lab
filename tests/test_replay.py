import pytest

from lab.replay import accept, done, parse_args, partition_ids, summarize


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


def test_done_when_every_partition_has_its_range():
    assert done({0: [0, 1, 2], 1: [0, 1]}, {0: 0, 1: 0}, {0: 3, 1: 2})


def test_not_done_while_a_partition_is_short():
    assert not done({0: [0, 1, 2], 1: [0]}, {0: 0, 1: 0}, {0: 3, 1: 2})


def test_done_with_empty_partition():
    assert done({0: []}, {0: 0}, {0: 0})


def test_done_with_log_start_offset_above_zero():
    # retention moved the log start to 5: only offsets 5..7 exist, so 3 records complete the partition
    assert done({0: [5, 6, 7]}, {0: 5}, {0: 8})
    assert not done({0: [5, 6]}, {0: 5}, {0: 8})


def test_accept_ignores_offsets_beyond_start_time_watermark_and_duplicates():
    seen = [0, 1, 2]
    assert accept(seen, 2, high=5) is False  # duplicate
    assert accept(seen, 5, high=5) is False  # arrived after the replay started
    assert accept(seen, 7, high=5) is False
    assert accept(seen, 3, high=5) is True
    assert accept([], 5, high=8) is True  # first record after a log start of 5


def test_partition_ids_sorted():
    assert partition_ids({"events": {2: None, 0: None, 1: None}}, "events") == [0, 1, 2]


def test_partition_ids_missing_topic_has_explicit_message():
    with pytest.raises(LookupError, match="topic 'events' not found"):
        partition_ids({}, "events")
