"""Measure produce-to-queryable delay for the broker-written Iceberg table.

Kafka evaluates the time-based segment roll when a record is appended, so after the marker we keep
appending one filler record per second until the segment containing the marker rolls and is offloaded.
With --filler-interval 0 no filler is sent (the marker is the only write), which measures the idle roll.
"""

import argparse
import json
import logging
import random
import sys
import time
import uuid
from datetime import datetime, timezone

from lab import endpoints
from lab.catalog import TableNotFound, table_metadata_location
from lab.events import load_schema, make_event_for
from lab.produce import build_producer, send
from lab.query import connect, render_sql

log = logging.getLogger(__name__)


def elapsed_since(start: float, now: float) -> float:
    if now < start:
        raise ValueError("now is before start")
    return round(now - start, 2)


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--segment-ms", type=int, default=10000, help="segment.ms the topic was created with (reported only)")
    p.add_argument("--poll", type=float, default=2.0, help="seconds between table checks when no filler is sent")
    p.add_argument("--timeout", type=float, default=300.0)
    p.add_argument(
        "--filler-interval",
        type=float,
        default=1.0,
        help="seconds between filler events after the marker (also the table-check interval); 0 = no filler",
    )
    p.add_argument("--bootstrap", default=endpoints.bootstrap())
    p.add_argument("--registry", default=endpoints.registry_url())
    p.add_argument("--rest", default=endpoints.catalog_url())
    p.add_argument("--minio", default=endpoints.minio_endpoint())
    return p.parse_args(argv)


def tick_seconds(args: argparse.Namespace) -> float:
    """Seconds between two table checks: the filler interval, or --poll when there is no filler."""
    return args.filler_interval if args.filler_interval > 0 else args.poll


def marker_visible(args: argparse.Namespace, marker: str) -> bool:
    try:
        location = table_metadata_location(args.rest, "default", "events")
    except TableNotFound:
        return False
    sql = render_sql(f"SELECT count(*) FROM {{table}} WHERE value.order_id = '{marker}'", location)
    return connect(args.minio).execute(sql).fetchone()[0] > 0  # fresh connection per check, as `make query` does


def measure(args: argparse.Namespace) -> dict:
    producer = build_producer(args.bootstrap, args.registry, load_schema("v1"), "avro")
    rng = random.Random(7)
    marker = f"marker-{uuid.uuid4().hex[:8]}"
    event = make_event_for("v1", 0, rng, int(time.time() * 1000))
    event["order_id"] = marker

    def on_delivery(err, _msg):
        if err is not None:
            log.error("delivery failed: %s", err)

    landed: dict[str, int] = {}

    def on_marker_delivery(err, msg):
        if err is not None:
            log.error("marker delivery failed: %s", err)
        else:
            landed.update(partition=msg.partition(), offset=msg.offset())

    send(producer, "events", event, on_marker_delivery)
    producer.flush(30)
    start = time.monotonic()
    produced_utc = utc_now()
    log.info(
        "marker %s produced at %s (partition %s, offset %s); waiting for it to become queryable",
        marker, produced_utc, landed.get("partition"), landed.get("offset"),
    )

    filler_sent = 0
    tick = tick_seconds(args)
    while time.monotonic() - start < args.timeout:
        if args.filler_interval > 0:
            filler_sent += 1
            send(producer, "events", make_event_for("v1", filler_sent, rng, int(time.time() * 1000)), on_delivery)
            producer.flush(10)
        if marker_visible(args, marker):
            return {
                "seconds_to_queryable": elapsed_since(start, time.monotonic()),
                "segment_ms": args.segment_ms,
                "filler_interval": args.filler_interval,
                "filler_sent": filler_sent,
                "marker": marker,
                "marker_partition": landed.get("partition"),
                "marker_offset": landed.get("offset"),
                "marker_produced_utc": produced_utc,
                "queryable_seen_utc": utc_now(),
            }
        time.sleep(tick)
    raise TimeoutError(f"marker {marker} not queryable after {args.timeout}s")


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        result = measure(parse_args(argv))
    except Exception as exc:
        log.error("freshness measurement failed: %s", exc)
        return 1
    log.info("queryable after %(seconds_to_queryable)s s (segment.ms=%(segment_ms)d)", result)
    print(json.dumps(result))  # machine-parsed stdout protocol
    return 0


if __name__ == "__main__":
    sys.exit(main())
