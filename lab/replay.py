"""Replay the events topic from offset 0 and report what the brokers served, per partition."""

import argparse
import json
import logging
import sys
import time

from confluent_kafka import OFFSET_BEGINNING, DeserializingConsumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

from lab.endpoints import bootstrap, registry_url

log = logging.getLogger(__name__)


def summarize(offsets: dict[int, list[int]], high_watermarks: dict[int, int], seconds: float) -> dict:
    partitions = {
        str(p): {
            "consumed": len(seen),
            "first_offset": seen[0] if seen else None,
            "last_offset": seen[-1] if seen else None,
            "high_watermark": high_watermarks[p],
        }
        for p, seen in sorted(offsets.items())
    }
    return {
        "consumed": sum(len(seen) for seen in offsets.values()),
        "partitions": partitions,
        "seconds": round(seconds, 2),
    }


def partition_ids(topics: dict, topic: str) -> list[int]:
    """Sorted partition ids of `topic`, or an explicit error when the broker does not know it."""
    if topic not in topics:
        raise LookupError(f"topic '{topic}' not found")
    return sorted(topics[topic])


def accept(seen: list[int], offset: int, high: int) -> bool:
    """True if `offset` is a new record below the watermark taken at replay start."""
    return offset < high and (not seen or offset > seen[-1])


def done(offsets: dict[int, list[int]], low: dict[int, int], high: dict[int, int]) -> bool:
    """True when every partition holds all of its records in [low, high)."""
    return all(len(offsets[p]) >= high[p] - low[p] for p in high)


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--topic", default="events")
    p.add_argument("--bootstrap", default=bootstrap())
    p.add_argument("--registry", default=registry_url())
    p.add_argument("--timeout", type=float, default=120.0)
    return p.parse_args(argv)


def replay(args: argparse.Namespace) -> dict:
    registry = SchemaRegistryClient({"url": args.registry})
    consumer = DeserializingConsumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": "lab-replay-unused",  # required by the client, never committed
            "enable.auto.commit": False,
            "key.deserializer": StringDeserializer("utf_8"),
            "value.deserializer": AvroDeserializer(registry),
        }
    )
    try:
        topics = {name: meta.partitions for name, meta in consumer.list_topics(args.topic, timeout=30).topics.items()}
        partitions = partition_ids(topics, args.topic)
        marks = {p: consumer.get_watermark_offsets(TopicPartition(args.topic, p), timeout=30) for p in partitions}
        low = {p: m[0] for p, m in marks.items()}
        high = {p: m[1] for p, m in marks.items()}
        consumer.assign([TopicPartition(args.topic, p, OFFSET_BEGINNING) for p in partitions])
        started = time.monotonic()
        offsets: dict[int, list[int]] = {p: [] for p in partitions}
        while not done(offsets, low, high):
            if time.monotonic() - started > args.timeout:
                got = sum(len(v) for v in offsets.values())
                raise TimeoutError(f"read {got} of {sum(high[p] - low[p] for p in partitions)} records in {args.timeout}s")
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise RuntimeError(str(msg.error()))
            if accept(offsets[msg.partition()], msg.offset(), high[msg.partition()]):
                offsets[msg.partition()].append(msg.offset())
        return summarize(offsets, high, time.monotonic() - started)
    finally:
        consumer.close()


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        summary = replay(parse_args(argv))
    except Exception as exc:
        log.error("replay failed: %s", exc)
        return 1
    log.info("replayed %(consumed)d records in %(seconds)s s", summary)
    print(json.dumps(summary))  # machine-parsed stdout protocol
    return 0


if __name__ == "__main__":
    sys.exit(main())
