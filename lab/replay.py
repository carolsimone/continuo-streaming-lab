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
        partitions = sorted(consumer.list_topics(args.topic, timeout=30).topics[args.topic].partitions)
        high = {p: consumer.get_watermark_offsets(TopicPartition(args.topic, p), timeout=30)[1] for p in partitions}
        consumer.assign([TopicPartition(args.topic, p, OFFSET_BEGINNING) for p in partitions])
        started = time.monotonic()
        offsets: dict[int, list[int]] = {p: [] for p in partitions}
        while any(len(offsets[p]) < high[p] for p in partitions):
            if time.monotonic() - started > args.timeout:
                done = sum(len(v) for v in offsets.values())
                raise TimeoutError(f"read {done} of {sum(high.values())} records in {args.timeout}s")
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise RuntimeError(str(msg.error()))
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
