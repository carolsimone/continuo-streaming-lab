"""Synthetic Avro producer for the streaming lab.

Writes `order_created` events to Kafka, registering the schema in Karapace.
Runs until EVENTS_COUNT events are sent (0 = unbounded) or until SIGTERM/SIGINT.
"""

import logging
import os
import random
import signal
import sys
import time
import uuid

from confluent_kafka import KafkaException, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

logging.basicConfig(
    stream=sys.stderr,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("producer")

ORDER_CREATED_SCHEMA = """
{
  "type": "record",
  "name": "OrderCreated",
  "namespace": "continuo.lab",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "customer_id", "type": "string"},
    {"name": "amount_cents", "type": "long"},
    {"name": "currency", "type": "string"},
    {"name": "created_at", "type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
"""

CURRENCIES = ("EUR", "USD", "GBP")
REPORT_EVERY = 1000

_stop = False


def _request_stop(signum: int, _frame: object) -> None:
    global _stop
    logger.info("received %s, stopping", signal.Signals(signum).name)
    _stop = True


def _on_delivery(err: object, msg: object) -> None:
    if err is not None:
        logger.error("delivery failed: %s", err)


def _make_event() -> tuple[str, dict]:
    order_id = str(uuid.uuid4())
    return order_id, {
        "order_id": order_id,
        "customer_id": f"c-{random.randint(1, 500):04d}",
        "amount_cents": random.randint(100, 50_000),
        "currency": random.choice(CURRENCIES),
        "created_at": int(time.time() * 1000),
    }


def main() -> int:
    bootstrap = os.environ["BOOTSTRAP_SERVERS"]
    registry_url = os.environ["SCHEMA_REGISTRY_URL"]
    topic = os.environ["TOPIC"]
    count = int(os.environ.get("EVENTS_COUNT", "0"))
    rate = float(os.environ.get("EVENTS_RATE", "50"))
    if rate <= 0:
        raise ValueError("EVENTS_RATE must be > 0")

    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)

    registry = SchemaRegistryClient({"url": registry_url})
    value_serializer = AvroSerializer(registry, ORDER_CREATED_SCHEMA)
    key_serializer = StringSerializer("utf-8")
    producer = Producer({"bootstrap.servers": bootstrap, "linger.ms": 20, "enable.idempotence": True})

    logger.info(
        "producing to %s at %s (rate=%.1f/s, count=%s)",
        topic, bootstrap, rate, count or "unbounded",
    )

    sent = 0
    interval = 1.0 / rate
    next_at = time.monotonic()
    while not _stop and (count == 0 or sent < count):
        key, event = _make_event()
        try:
            producer.produce(
                topic=topic,
                key=key_serializer(key),
                value=value_serializer(event, SerializationContext(topic, MessageField.VALUE)),
                on_delivery=_on_delivery,
            )
        except (BufferError, KafkaException) as exc:
            logger.warning("produce failed, retrying after poll: %s", exc)
            producer.poll(1.0)
            continue
        sent += 1
        producer.poll(0)
        if sent % REPORT_EVERY == 0:
            logger.info("sent %d events", sent)

        next_at += interval
        delay = next_at - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        else:
            next_at = time.monotonic()  # fell behind; do not burst to catch up

    remaining = producer.flush(10)
    if remaining:
        logger.error("%d messages were not delivered before exit", remaining)
        return 1
    logger.info("done: %d events sent", sent)
    return 0


if __name__ == "__main__":
    sys.exit(main())
