"""Produce synthetic order events to Kafka as Avro, registering the schema in Karapace."""

import argparse
import itertools
import json
import logging
import os
import random
import signal
import sys
import threading
import time

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

from lab import endpoints
from lab.events import SCHEMA_VERSIONS, load_schema, make_event_for

log = logging.getLogger(__name__)

KEY_SERDES = ("string", "avro")
KEY_FIELDS = ("order_id", "customer_id")
KEY_SCHEMA_STR = '{"type": "string"}'

FLUSH_TIMEOUT_S = 60  # normal finite run: wait for every message
STOP_FLUSH_TIMEOUT_S = 10  # after SIGTERM: must finish inside compose.yml stop_grace_period (15s)


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--count", type=int, default=20000, help="events to send; 0 = run until SIGTERM/SIGINT")
    p.add_argument("--rate", type=float, default=0.0, help="events/second, 0 = unthrottled")
    p.add_argument("--schema-version", default="v1", help=f"one of {SCHEMA_VERSIONS}")
    p.add_argument("--start-seq", type=int, default=0)
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--topic", default="events")
    p.add_argument("--bootstrap", default=endpoints.bootstrap())
    p.add_argument("--registry", default=endpoints.registry_url())
    p.add_argument(
        "--key-serde",
        choices=KEY_SERDES,
        default=os.environ.get("LAB_KEY_SERDE") or "string",  # empty = unset (make/compose pass "")
        help="record key encoding: plain UTF-8 'string', or 'avro' (needed by the Iceberg mode of the plugin) "
        "[env LAB_KEY_SERDE]",
    )
    p.add_argument(
        "--key-field",
        choices=KEY_FIELDS,
        default=os.environ.get("LAB_KEY_FIELD") or "order_id",
        help="event field used as record key; customer_id repeats across events [env LAB_KEY_FIELD]",
    )
    args = p.parse_args(argv)
    for name, allowed in (("key_serde", KEY_SERDES), ("key_field", KEY_FIELDS)):
        if getattr(args, name) not in allowed:  # argparse does not check choices for a default
            p.error(f"invalid --{name.replace('_', '-')}: {getattr(args, name)!r} (choose from {', '.join(allowed)})")
    return args


def build_producer(
    bootstrap: str, registry_url: str, schema_str: str, key_serde: str = "string"
) -> SerializingProducer:
    registry = SchemaRegistryClient({"url": registry_url})
    registry.get_subjects()  # preflight: fail fast if Karapace is unreachable
    if key_serde == "avro":
        # Registered under the default topic naming strategy as `<topic>-key`.
        key_serializer = AvroSerializer(registry, KEY_SCHEMA_STR)
    else:
        key_serializer = StringSerializer("utf_8")
    return SerializingProducer(
        {
            "bootstrap.servers": bootstrap,
            "key.serializer": key_serializer,
            "value.serializer": AvroSerializer(registry, schema_str),
            "linger.ms": 20,
            "enable.idempotence": True,  # retries must not duplicate records (matters during leader failover)
        }
    )


def send(
    producer: SerializingProducer,
    topic: str,
    event: dict,
    on_delivery,
    stop: threading.Event | None = None,
    key_field: str = "order_id",
) -> bool:
    """Enqueue one event, retrying while the local queue is full.

    Returns True once enqueued. If `stop` is set while waiting for queue space, gives up and
    returns False (the event was not enqueued).
    """
    while True:
        try:
            producer.produce(topic=topic, key=event[key_field], value=event, on_delivery=on_delivery)
            producer.poll(0)
            return True
        except BufferError:
            producer.poll(0.5)
            if stop is not None and stop.is_set():
                return False


def run(args: argparse.Namespace, stop: threading.Event) -> dict:
    """Send events until `args.count` is reached (0 = unbounded) or `stop` is set, then flush."""
    schema_str = load_schema(args.schema_version)
    producer = build_producer(args.bootstrap, args.registry, schema_str, args.key_serde)
    rng = random.Random(args.seed)
    counts = {"produced": 0, "failed": 0}

    def on_delivery(err, msg):
        if err is not None:
            counts["failed"] += 1
            log.error("delivery failed: %s", err)
        else:
            counts["produced"] += 1

    interval = 1.0 / args.rate if args.rate > 0 else 0.0
    first = args.start_seq
    sent = 0
    seqs = itertools.count(first) if args.count == 0 else range(first, first + args.count)
    for seq in seqs:
        if stop.is_set():
            log.info("stop requested after %d events", sent)
            break
        event = make_event_for(args.schema_version, seq, rng, int(time.time() * 1000))
        if not send(producer, args.topic, event, on_delivery, stop, args.key_field):
            log.warning("stop requested while the producer queue was full; event %d not sent", seq)
            break
        sent += 1
        if interval:
            stop.wait(interval)  # interruptible sleep: a stop request is honoured immediately
    flush_timeout = STOP_FLUSH_TIMEOUT_S if stop.is_set() else FLUSH_TIMEOUT_S
    remaining = producer.flush(flush_timeout)
    if remaining:
        counts["failed"] += remaining
        log.error("%d messages were not delivered within the %ds flush timeout", remaining, flush_timeout)
    return {**counts, "first_seq": first, "last_seq": first + sent - 1}


def _new_stop_event() -> threading.Event:
    return threading.Event()


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args(argv)
    stop = _new_stop_event()

    def request_stop(signum, _frame):
        log.info("received %s, stopping", signal.Signals(signum).name)
        stop.set()

    previous = {sig: signal.signal(sig, request_stop) for sig in (signal.SIGTERM, signal.SIGINT)}
    try:
        summary = run(args, stop)
    except Exception as exc:  # report once, exit non-zero; no traceback spam for expected failures
        log.error("produce failed: %s", exc)
        return 1
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
    log.info("produced=%(produced)d failed=%(failed)d", summary)
    print(json.dumps(summary))  # machine-parsed stdout protocol (see Global Constraints)
    return 0 if summary["failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
