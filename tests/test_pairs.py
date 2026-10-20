import json
from lab import pairs
from lab.pairs import compare, missing_ranges, parse_args, replay_done


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
    seen = {0: [0, 1, 2], 1: [0]}
    assert not replay_done(seen, {0: 0, 1: 0}, {0: 3, 1: 2})
    seen[1].append(1)
    assert replay_done(seen, {0: 0, 1: 0}, {0: 3, 1: 2})


def test_replay_done_empty_partition_is_done():
    assert replay_done({0: []}, {0: 4}, {0: 4})


def test_parse_args_defaults():
    args = parse_args([])
    assert args.topic == "events"
    assert args.bootstrap == "localhost:9092,localhost:9192,localhost:9292"
    assert args.rest == "http://localhost:8181"
    assert args.timeout == 180.0


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
