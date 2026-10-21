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

from confluent_kafka import OFFSET_BEGINNING, DeserializingConsumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from lab import endpoints
from lab.catalog import TableNotFound, table_metadata_location
from lab.query import connect, render_sql
from lab.replay import partition_ids

log = logging.getLogger(__name__)

Pair = tuple[int, int]
Triple = tuple[int, int, str]  # (partition, offset, order_id)
MAX_RANGES = 10  # missing offsets are reported as runs, at most this many per partition

COUNT_KEYS = (
    "expected", "table_rows", "table_distinct", "table_duplicates", "table_missing", "table_beyond_end",
    "replay_rows", "replay_distinct", "replay_duplicates", "replay_missing", "replay_beyond_end",
    "table_only", "replay_only",
)  # fmt: skip


def retry(fn, attempts: int, delay: float, what: str):
    """Call fn(); on an exception wait `delay` seconds and try again, up to `attempts` calls in all."""
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:
            if attempt == attempts:
                raise
            log.warning("%s failed (%s); retry %d of %d", what, exc, attempt, attempts - 1)
            time.sleep(delay)


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


TABLE_KEYS = ("table_rows", "table_distinct", "table_duplicates", "table_missing", "table_beyond_end", "table_only",
              "replay_only")


def compare(table: list[Pair] | None, replayed: list[Pair], low: dict[int, int], high: dict[int, int]) -> dict:
    """Per-partition and total duplicate/gap counts of the table and the replay against [low, high).

    `table` None means the table could not be read: its counts (and the table/replay overlap) are None and the
    replay is still checked against the end offsets.
    """
    table_by: dict[int, list[int]] = {p: [] for p in high}
    replay_by: dict[int, list[int]] = {p: [] for p in high}
    for source, by in ((table or [], table_by), (replayed, replay_by)):
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
        if table is None:
            partitions[str(p)].update(dict.fromkeys(TABLE_KEYS + ("table_missing_ranges",)))
    totals = {
        k: None if table is None and k in TABLE_KEYS else sum(v[k] for v in partitions.values()) for k in COUNT_KEYS
    }
    return {"partitions": partitions, "totals": totals}


def content_report(table: list[Triple], replayed: list[Triple]) -> dict:
    """Does the same (partition, offset) carry the same record (order_id) in the table and in the replay?

    Offsets present on both sides are compared (the first delivery of an offset wins in the replay).
    Repeated order_ids are counted per source: each event of the lab carries a unique order_id. Order_ids found
    on one side only (per partition, each counted once) tell a record that never came back (table only) from a
    record the table does not hold (replay only).
    """
    first: dict[Pair, str] = {}
    for p, o, order_id in replayed:
        first.setdefault((p, o), order_id)
    table_ids: dict[int, list[str]] = {}
    replay_ids: dict[int, list[str]] = {}
    different: dict[int, int] = {}
    for p, o, order_id in table:
        table_ids.setdefault(p, []).append(order_id)
        if (p, o) in first and first[(p, o)] != order_id:
            different[p] = different.get(p, 0) + 1
    for p, _o, order_id in replayed:
        replay_ids.setdefault(p, []).append(order_id)
    partitions = {}
    for p in sorted(table_ids.keys() | replay_ids.keys()):
        t_ids, r_ids = table_ids.get(p, []), replay_ids.get(p, [])
        partitions[str(p)] = {
            "offsets_with_different_record": different.get(p, 0),
            "table_duplicate_order_ids": len(t_ids) - len(set(t_ids)),
            "replay_duplicate_order_ids": len(r_ids) - len(set(r_ids)),
            "table_order_ids_not_in_replay": len(set(t_ids) - set(r_ids)),
            "replay_order_ids_not_in_table": len(set(r_ids) - set(t_ids)),
        }
    keys = (
        "offsets_with_different_record", "table_duplicate_order_ids", "replay_duplicate_order_ids",
        "table_order_ids_not_in_replay", "replay_order_ids_not_in_table",
    )  # fmt: skip
    return {"partitions": partitions, "totals": {k: sum(v[k] for v in partitions.values()) for k in keys}}


def replay_done(last: dict[int, int | None], low: dict[int, int], high: dict[int, int]) -> bool:
    """True when every non-empty partition delivered an offset >= its end offset - 1.

    `last` holds the highest offset delivered so far per partition (None before the first record).
    """
    return all(high[p] <= low[p] or (last[p] is not None and last[p] >= high[p] - 1) for p in high)


def progress(last: dict[int, int | None], high: dict[int, int]) -> str:
    """One-line state of the replay: highest offset delivered per partition and the end offset."""
    return "; ".join(f"p{p} last {'none' if last[p] is None else last[p]} of end {high[p]}" for p in sorted(high))


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--topic", default="events")
    p.add_argument("--bootstrap", default=endpoints.bootstrap())
    p.add_argument("--registry", default=endpoints.registry_url())
    p.add_argument("--rest", default=endpoints.catalog_url())
    p.add_argument("--minio", default=endpoints.minio_endpoint())
    p.add_argument("--timeout", type=float, default=180.0, help="seconds allowed for the replay read")
    p.add_argument(
        "--idle-timeout", type=float, default=30.0, help="stop the replay after this many seconds without a record"
    )
    p.add_argument("--debug-fetch", action="store_true", help="turn on librdkafka's 'fetch' debug log (stderr)")
    return p.parse_args(argv)


def table_triples(args: argparse.Namespace) -> list[Triple]:
    location = table_metadata_location(args.rest, "default", "events")
    sql = render_sql('SELECT kafka.partition, kafka."offset", value.order_id FROM {table}', location)
    return [(int(p), int(o), str(i)) for p, o, i in connect(args.minio).execute(sql).fetchall()]


def replay_pairs(
    consumer: DeserializingConsumer, topic: str, low: dict[int, int], high: dict[int, int], timeout: float, idle: float
) -> tuple[list[Triple], str, dict[str, int]]:
    """Read every partition from the beginning; keep every delivered record, duplicates included.

    Returns the pairs, a status ("complete", or why the read stopped early: a replay that stalls below the end
    offsets is a result, not an error, and its shortfall shows up as missing pairs) and the consumer's position
    per partition when the read ended (the offset it would fetch next).
    """
    consumer.assign([TopicPartition(topic, p, OFFSET_BEGINNING) for p in high])
    last: dict[int, int | None] = {p: None for p in high}  # running maximum: no rescan of the pairs per poll
    pairs: list[Triple] = []
    started = last_message = time.monotonic()
    status = "complete"
    while not replay_done(last, low, high):
        now = time.monotonic()
        if now - started > timeout:
            status = f"timeout after {timeout}s"
        elif now - last_message > idle:
            status = f"no record for {idle}s"
        if status != "complete":
            log.error("replay stopped early (%s) after %d records: %s", status, len(pairs), progress(last, high))
            break
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            raise RuntimeError(str(msg.error()))
        last_message = time.monotonic()
        p, o = msg.partition(), msg.offset()
        if last.get(p) is None or o > last[p]:
            last[p] = o
        pairs.append((p, o, msg.value()["order_id"]))
    positions = {str(tp.partition): tp.offset for tp in consumer.position([TopicPartition(topic, p) for p in high])}
    log.info("replay read %d records in %.1f s (%s); consumer positions %s", len(pairs), time.monotonic() - started,
             status, positions)
    return pairs, status, positions


def collect(args: argparse.Namespace) -> dict:
    conf = {
        "value.deserializer": AvroDeserializer(SchemaRegistryClient({"url": args.registry})),
        "bootstrap.servers": args.bootstrap,
        "group.id": "lab-pairs-unused",  # required by the client, never committed
        "enable.auto.commit": False,
    }
    if args.debug_fetch:
        conf.update({"debug": "fetch", "logger": log})  # librdkafka's fetch decisions, through this module's logger
    consumer = DeserializingConsumer(conf)
    try:
        topics = retry(lambda: consumer.list_topics(args.topic, timeout=30).topics, 4, 3.0, "topic metadata")
        ids = partition_ids(topics, args.topic)

        def watermarks(p: int) -> tuple[int, int]:
            return retry(
                lambda: consumer.get_watermark_offsets(TopicPartition(args.topic, p), timeout=30, cached=False),
                4, 3.0, f"watermarks of partition {p}",
            )  # fmt: skip

        marks = {p: watermarks(p) for p in ids}
        low = {p: m[0] for p, m in marks.items()}
        high = {p: m[1] for p, m in marks.items()}
        log.info("log start offsets %s, end offsets %s", low, high)
        try:
            table, table_status = table_triples(args), "ok"
            log.info("table holds %d rows", len(table))
        except TableNotFound as exc:  # no table yet (nothing uploaded): still check the replay
            table, table_status = None, f"not found: {exc}"
            log.warning("table %s; comparing the replay with the end offsets only", table_status)
        replayed, status, positions = replay_pairs(consumer, args.topic, low, high, args.timeout, args.idle_timeout)
    finally:
        consumer.close()
    as_pairs = lambda triples: [(p, o) for p, o, _ in triples]  # noqa: E731
    return {
        **compare(None if table is None else as_pairs(table), as_pairs(replayed), low, high),
        "content": None if table is None else content_report(table, replayed),
        "table_status": table_status,
        "replay_status": status,
        "replay_positions": positions,
    }


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        result = collect(parse_args(argv))
    except Exception as exc:
        log.error("pairs check failed: %s", exc)
        return 1
    totals = result["totals"]
    log.info(
        "table duplicates %s, table missing %s, replay duplicates %s, replay missing %s",
        totals["table_duplicates"], totals["table_missing"], totals["replay_duplicates"], totals["replay_missing"],
    )  # fmt: skip
    print(json.dumps(result))  # machine-parsed stdout protocol
    return 0


if __name__ == "__main__":
    sys.exit(main())
