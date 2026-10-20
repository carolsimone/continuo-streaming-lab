"""Compare record identities, (partition, offset) pairs, three ways: Iceberg table, topic replay, broker offsets.

A record is identified by (partition, offset), never by offset alone. The table pairs come from the
broker-written Iceberg table (DuckDB), the replay pairs from a consumer reading every partition from
offset 0 up to the end offset taken at the start (so history is served by whichever broker leads the
partition now), and the expected range [log start, end offset) from the brokers' watermarks.
Duplicates are counted on raw rows; replay deduplication is deliberately not applied.
"""

import argparse
import json
import logging
import sys
import time

from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

from lab import endpoints
from lab.catalog import table_metadata_location
from lab.query import connect, render_sql
from lab.replay import partition_ids

log = logging.getLogger(__name__)

Pair = tuple[int, int]
MAX_RANGES = 10  # missing offsets are reported as runs, at most this many per partition

COUNT_KEYS = (
    "expected", "table_rows", "table_distinct", "table_duplicates", "table_missing", "table_beyond_end",
    "replay_rows", "replay_distinct", "replay_duplicates", "replay_missing", "replay_beyond_end",
    "table_only", "replay_only",
)  # fmt: skip


def missing_ranges(present: set[int], low: int, high: int, limit: int = MAX_RANGES) -> list[list[int]]:
    """Runs [first, last] of offsets in [low, high) absent from `present`, at most `limit` runs."""
    runs: list[list[int]] = []
    start = None
    for offset in range(low, high + 1):
        absent = offset < high and offset not in present
        if absent and start is None:
            start = offset
        elif not absent and start is not None:
            runs.append([start, offset - 1])
            start = None
            if len(runs) >= limit:
                break
    return runs


def compare(table: list[Pair], replayed: list[Pair], low: dict[int, int], high: dict[int, int]) -> dict:
    """Per-partition and total duplicate/gap counts for the table and the replay against [low, high)."""
    table_by: dict[int, list[int]] = {p: [] for p in high}
    replay_by: dict[int, list[int]] = {p: [] for p in high}
    for source, by in ((table, table_by), (replayed, replay_by)):
        for partition, offset in source:
            by.setdefault(partition, []).append(offset)
    partitions: dict[str, dict] = {}
    for p in sorted(table_by.keys() | replay_by.keys() | high.keys()):
        lo, hi = low.get(p, 0), high.get(p, 0)
        t, r = table_by.get(p, []), replay_by.get(p, [])
        t_set, r_set = set(t), set(r)
        in_range = lambda s: {o for o in s if lo <= o < hi}  # noqa: E731
        t_in, r_in = in_range(t_set), in_range(r_set)
        partitions[str(p)] = {
            "low": lo,
            "end_offset": hi,
            "expected": hi - lo,
            "table_rows": len(t),
            "table_distinct": len(t_set),
            "table_duplicates": len(t) - len(t_set),
            "table_missing": (hi - lo) - len(t_in),
            "table_missing_ranges": missing_ranges(t_set, lo, hi),
            "table_beyond_end": len(t_set) - len(t_in),
            "replay_rows": len(r),
            "replay_distinct": len(r_set),
            "replay_duplicates": len(r) - len(r_set),
            "replay_missing": (hi - lo) - len(r_in),
            "replay_missing_ranges": missing_ranges(r_set, lo, hi),
            "replay_beyond_end": len(r_set) - len(r_in),
            "table_only": len(t_in - r_in),
            "replay_only": len(r_in - t_in),
        }
    totals = {k: sum(v[k] for v in partitions.values()) for k in COUNT_KEYS}
    return {"partitions": partitions, "totals": totals}


def replay_done(seen: dict[int, list[int]], low: dict[int, int], high: dict[int, int]) -> bool:
    """True when every non-empty partition has delivered an offset >= end offset - 1."""
    return all(high[p] <= low[p] or (seen[p] and max(seen[p]) >= high[p] - 1) for p in high)


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--topic", default="events")
    p.add_argument("--bootstrap", default=endpoints.bootstrap())
    p.add_argument("--rest", default=endpoints.catalog_url())
    p.add_argument("--minio", default=endpoints.minio_endpoint())
    p.add_argument("--timeout", type=float, default=180.0, help="seconds allowed for the replay read")
    return p.parse_args(argv)


def table_pairs(args: argparse.Namespace) -> list[Pair]:
    location = table_metadata_location(args.rest, "default", "events")
    sql = render_sql('SELECT kafka.partition, kafka."offset" FROM {table}', location)
    return [(int(p), int(o)) for p, o in connect(args.minio).execute(sql).fetchall()]


def replay_pairs(consumer: Consumer, topic: str, low: dict[int, int], high: dict[int, int], timeout: float) -> list[Pair]:
    """Read every partition from the beginning; keep every delivered (partition, offset), duplicates included."""
    consumer.assign([TopicPartition(topic, p, OFFSET_BEGINNING) for p in high])
    seen: dict[int, list[int]] = {p: [] for p in high}
    pairs: list[Pair] = []
    started = time.monotonic()
    while not replay_done(seen, low, high):
        if time.monotonic() - started > timeout:
            raise TimeoutError(f"replay read {len(pairs)} records in {timeout}s; not at the end offsets {high}")
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            raise RuntimeError(str(msg.error()))
        seen[msg.partition()].append(msg.offset())
        pairs.append((msg.partition(), msg.offset()))
    log.info("replay read %d records in %.1f s", len(pairs), time.monotonic() - started)
    return pairs


def collect(args: argparse.Namespace) -> dict:
    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": "lab-pairs-unused",  # required by the client, never committed
            "enable.auto.commit": False,
            "enable.partition.eof": False,
        }
    )
    try:
        ids = partition_ids(consumer.list_topics(args.topic, timeout=30).topics, args.topic)
        marks = {p: consumer.get_watermark_offsets(TopicPartition(args.topic, p), timeout=30, cached=False) for p in ids}
        low = {p: m[0] for p, m in marks.items()}
        high = {p: m[1] for p, m in marks.items()}
        log.info("log start offsets %s, end offsets %s", low, high)
        table = table_pairs(args)
        log.info("table holds %d rows", len(table))
        replayed = replay_pairs(consumer, args.topic, low, high, args.timeout)
    finally:
        consumer.close()
    return compare(table, replayed, low, high)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        result = collect(parse_args(argv))
    except Exception as exc:
        log.error("pairs check failed: %s", exc)
        return 1
    totals = result["totals"]
    log.info(
        "table duplicates %d, table missing %d, replay duplicates %d, replay missing %d",
        totals["table_duplicates"], totals["table_missing"], totals["replay_duplicates"], totals["replay_missing"],
    )  # fmt: skip
    print(json.dumps(result))  # machine-parsed stdout protocol
    return 0


if __name__ == "__main__":
    sys.exit(main())
