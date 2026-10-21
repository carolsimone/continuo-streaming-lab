import json

import pytest

from confluent_kafka import TopicPartition

from lab import pairs
from lab.catalog import TableNotFound
from lab.pairs import compare, missing_ranges, parse_args, progress, replay_done


def test_missing_ranges_collapses_runs():
    assert missing_ranges({0, 1, 4, 7, 8}, 0, 10) == [[2, 3], [5, 6], [9, 9]]


def test_missing_ranges_none_when_complete():
    assert missing_ranges({0, 1, 2}, 0, 3) == []


def test_missing_ranges_respects_low_bound_and_empty_partition():
    assert missing_ranges({5, 6}, 5, 8) == [[7, 7]]
    assert missing_ranges(set(), 3, 3) == []


def test_missing_ranges_caps_the_list():
    got = missing_ranges(set(), 0, 100, limit=2)
    assert got == [[0, 99]]  # one run is one entry
    got = missing_ranges(set(range(0, 100, 2)), 0, 100, limit=3)
    assert len(got) == 3


def test_compare_clean_run_has_no_gaps_or_duplicates():
    p = [(0, 0), (0, 1), (1, 0)]
    r = compare(p, p, {0: 0, 1: 0}, {0: 2, 1: 1})
    assert r["totals"]["table_duplicates"] == 0
    assert r["totals"]["table_missing"] == 0
    assert r["totals"]["replay_missing"] == 0
    assert r["totals"]["table_only"] == 0 and r["totals"]["replay_only"] == 0
    assert r["partitions"]["0"]["expected"] == 2


def test_compare_counts_duplicates_and_gaps_per_partition():
    table = [(0, 0), (0, 1), (0, 1), (0, 3), (1, 0)]
    replayed = [(0, 0), (0, 1), (0, 2), (0, 3), (1, 0)]
    r = compare(table, replayed, {0: 0, 1: 0}, {0: 4, 1: 1})
    p0 = r["partitions"]["0"]
    assert (p0["table_rows"], p0["table_distinct"], p0["table_duplicates"]) == (4, 3, 1)
    assert (p0["table_missing"], p0["table_missing_ranges"]) == (1, [[2, 2]])
    assert (p0["replay_missing"], p0["replay_duplicates"]) == (0, 0)
    assert (p0["table_only"], p0["replay_only"]) == (0, 1)
    assert r["partitions"]["1"]["table_missing"] == 0
    assert r["totals"]["table_duplicates"] == 1
    assert r["totals"]["table_missing"] == 1


def test_compare_counts_pairs_beyond_the_end_offset():
    r = compare([(0, 0), (0, 5)], [(0, 0)], {0: 0}, {0: 1})
    assert r["partitions"]["0"]["table_beyond_end"] == 1
    assert r["partitions"]["0"]["table_missing"] == 0


def test_compare_nonzero_log_start():
    r = compare([(0, 2), (0, 3)], [(0, 2), (0, 3)], {0: 2}, {0: 4})
    assert r["partitions"]["0"]["expected"] == 2
    assert r["totals"]["table_missing"] == 0


def test_replay_done_needs_the_last_offset_of_every_partition():
    last = {0: 2, 1: 0}  # highest offset delivered so far per partition
    assert not replay_done(last, {0: 0, 1: 0}, {0: 3, 1: 2})
    last[1] = 1
    assert replay_done(last, {0: 0, 1: 0}, {0: 3, 1: 2})


def test_replay_done_empty_partition_is_done():
    assert replay_done({0: None}, {0: 4}, {0: 4})


def test_replay_done_nothing_delivered_yet_is_not_done():
    assert not replay_done({0: None}, {0: 0}, {0: 4})


def test_parse_args_defaults():
    args = parse_args([])
    assert args.topic == "events"
    assert args.bootstrap == "localhost:9092,localhost:9192,localhost:9292"
    assert args.rest == "http://localhost:8181"
    assert args.registry == "http://localhost:8081"
    assert (args.timeout, args.idle_timeout) == (180.0, 30.0)


def test_main_prints_one_json_line(monkeypatch, capsys):
    result = {"partitions": {}, "totals": dict.fromkeys(pairs.COUNT_KEYS, 0)}
    monkeypatch.setattr(pairs, "collect", lambda _args: result)
    assert pairs.main([]) == 0
    assert json.loads(capsys.readouterr().out) == result


def test_main_failure_exits_1_with_empty_stdout(monkeypatch, capsys):
    def boom(_args):
        raise RuntimeError("no table")

    monkeypatch.setattr(pairs, "collect", boom)
    assert pairs.main([]) == 1
    assert capsys.readouterr().out == ""


def test_progress_names_last_offset_and_end_offset_per_partition():
    assert progress({0: 2, 1: None}, {0: 5, 1: 2}) == "p0 last 2 of end 5; p1 last none of end 2"


class FakeMsg:
    def __init__(self, partition, offset):
        self._p, self._o = partition, offset

    def error(self):
        return None

    def partition(self):
        return self._p

    def offset(self):
        return self._o

    def value(self):
        return {"order_id": f"o-{self._o}"}


class FakeConsumer:
    def __init__(self, msgs, positions=None):
        self.msgs = list(msgs)
        self.assigned = None
        self.positions = positions or {}

    def assign(self, tps):
        self.assigned = tps

    def poll(self, _timeout):
        return self.msgs.pop(0) if self.msgs else None

    def position(self, tps):
        return [TopicPartition(tp.topic, tp.partition, self.positions.get(tp.partition, -1001)) for tp in tps]


def test_replay_pairs_complete_keeps_raw_duplicates():
    consumer = FakeConsumer([FakeMsg(0, 0), FakeMsg(0, 1), FakeMsg(0, 1), FakeMsg(0, 2)])
    got, status, _positions = pairs.replay_pairs(consumer, "events", {0: 0}, {0: 3}, timeout=5, idle=5)
    assert status == "complete"
    assert got == [(0, 0, "o-0"), (0, 1, "o-1"), (0, 1, "o-1"), (0, 2, "o-2")]
    assert len(consumer.assigned) == 1


def test_replay_pairs_stalled_below_the_end_is_reported_not_raised(monkeypatch):
    monkeypatch.setattr(pairs.time, "monotonic", iter(range(0, 1000)).__next__)
    consumer = FakeConsumer([FakeMsg(0, 0), FakeMsg(0, 1)], positions={0: 2})
    got, status, positions = pairs.replay_pairs(consumer, "events", {0: 0}, {0: 5}, timeout=500, idle=10)
    assert got == [(0, 0, "o-0"), (0, 1, "o-1")]
    assert status == "no record for 10s"
    assert positions == {"0": 2}  # where the consumer would fetch next when it stopped


def test_content_report_flags_same_offset_with_different_record():
    table = [(0, 0, "a"), (0, 1, "b"), (0, 2, "c"), (1, 0, "x")]
    replayed = [(0, 0, "a"), (0, 1, "c"), (0, 2, "c"), (1, 0, "x")]
    r = pairs.content_report(table, replayed)
    assert r["partitions"]["0"] == {
        "offsets_with_different_record": 1,
        "table_duplicate_order_ids": 0,
        "replay_duplicate_order_ids": 1,
        "table_order_ids_not_in_replay": 1,  # "b" never came back
        "replay_order_ids_not_in_table": 0,
    }
    assert r["partitions"]["1"]["offsets_with_different_record"] == 0
    assert r["totals"] == {
        "offsets_with_different_record": 1,
        "table_duplicate_order_ids": 0,
        "replay_duplicate_order_ids": 1,
        "table_order_ids_not_in_replay": 1,
        "replay_order_ids_not_in_table": 0,
    }


def test_content_report_ignores_offsets_missing_on_one_side():
    r = pairs.content_report([(0, 0, "a")], [(0, 5, "z")])
    assert r["totals"]["offsets_with_different_record"] == 0


def test_content_report_first_delivery_of_an_offset_is_the_one_compared():
    r = pairs.content_report([(0, 0, "a")], [(0, 0, "a"), (0, 0, "b")])
    assert r["totals"]["offsets_with_different_record"] == 0
    assert r["totals"]["replay_duplicate_order_ids"] == 0


def test_retry_returns_after_transient_failures(monkeypatch):
    monkeypatch.setattr(pairs.time, "sleep", lambda _s: None)
    calls = []

    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise RuntimeError("transient")
        return "ok"

    assert pairs.retry(flaky, attempts=5, delay=0.0, what="x") == "ok"
    assert len(calls) == 3


def test_retry_gives_up_with_the_last_error(monkeypatch):
    monkeypatch.setattr(pairs.time, "sleep", lambda _s: None)

    def always():
        raise RuntimeError("down")

    with pytest.raises(RuntimeError, match="down"):
        pairs.retry(always, attempts=2, delay=0.0, what="x")


def test_content_report_counts_order_ids_on_one_side_only():
    r = pairs.content_report([(0, 0, "a"), (0, 1, "b")], [(0, 0, "a"), (0, 1, "z"), (0, 1, "z")])
    assert r["totals"]["table_order_ids_not_in_replay"] == 1  # "b"
    assert r["totals"]["replay_order_ids_not_in_table"] == 1  # "z", counted once


def test_compare_without_table_reports_replay_against_end_offsets_only():
    r = compare(None, [(0, 0), (0, 1), (0, 1)], {0: 0}, {0: 3})
    p0 = r["partitions"]["0"]
    assert (p0["replay_rows"], p0["replay_duplicates"], p0["replay_missing"]) == (3, 1, 1)
    assert p0["table_rows"] is None and p0["table_missing"] is None and p0["table_only"] is None
    assert r["totals"]["table_rows"] is None
    assert r["totals"]["replay_missing"] == 1


def test_parse_args_debug_fetch_is_off_by_default():
    assert parse_args([]).debug_fetch is False
    assert parse_args(["--debug-fetch"]).debug_fetch is True


class FakeMeta:
    error = None
    partitions = {0: object()}


class FakeTopics:
    topics = {"events": FakeMeta()}


def _collect_with_fakes(monkeypatch, table_side, argv=()):
    built = {}

    class FakeDeserializingConsumer:
        def __init__(self, conf):
            built["conf"] = conf

        def list_topics(self, _topic, timeout):
            return FakeTopics()

        def get_watermark_offsets(self, _tp, timeout, cached):
            return (0, 2)

        def close(self):
            built["closed"] = True

    monkeypatch.setattr(pairs, "DeserializingConsumer", FakeDeserializingConsumer)
    monkeypatch.setattr(pairs, "SchemaRegistryClient", lambda conf: object())
    monkeypatch.setattr(pairs, "AvroDeserializer", lambda registry: object())
    monkeypatch.setattr(pairs, "table_triples", table_side)
    replayed = [(0, 0, "o-0"), (0, 1, "o-1")]
    monkeypatch.setattr(pairs, "replay_pairs", lambda *a, **k: (replayed, "complete", {"0": 2}))
    return pairs.collect(parse_args(list(argv))), built


def test_collect_without_the_table_still_reports_the_replay(monkeypatch):
    def no_table(_args):
        raise TableNotFound("default.events")

    result, built = _collect_with_fakes(monkeypatch, no_table)
    assert result["table_status"] == "not found: default.events"
    assert result["content"] is None
    assert result["totals"]["replay_rows"] == 2 and result["totals"]["replay_missing"] == 0
    assert result["replay_positions"] == {"0": 2}
    assert built["closed"]


def test_collect_with_the_table_compares_both_sides(monkeypatch):
    result, _ = _collect_with_fakes(monkeypatch, lambda _args: [(0, 0, "o-0"), (0, 1, "o-1")])
    assert result["table_status"] == "ok"
    assert result["totals"]["table_rows"] == 2
    assert result["content"]["totals"]["offsets_with_different_record"] == 0


def test_collect_debug_fetch_turns_on_librdkafka_fetch_debug(monkeypatch):
    _, built = _collect_with_fakes(monkeypatch, lambda _args: [], argv=["--debug-fetch"])
    assert built["conf"]["debug"] == "fetch"
    _, built = _collect_with_fakes(monkeypatch, lambda _args: [])
    assert "debug" not in built["conf"]


def test_replay_pairs_running_maximum_ignores_a_lower_offset_after_a_higher_one(monkeypatch, caplog):
    # End offset 4, not 3: with 3 the replay is complete once offset 2 arrives and offset 1 is never polled.
    monkeypatch.setattr(pairs.time, "monotonic", iter(range(0, 1000)).__next__)
    consumer = FakeConsumer([FakeMsg(0, 0), FakeMsg(0, 2), FakeMsg(0, 1)], positions={0: 3})
    with caplog.at_level("ERROR", logger=pairs.log.name):
        got, status, _ = pairs.replay_pairs(consumer, "events", {0: 0}, {0: 4}, timeout=500, idle=10)
    assert [o for _, o, _ in got] == [0, 2, 1]  # every delivery kept, in order
    assert status == "no record for 10s"
    assert "p0 last 2 of end 4" in caplog.text  # the maximum stays 2 after offset 1 arrives
