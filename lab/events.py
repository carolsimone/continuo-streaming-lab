"""Synthetic order events and their Avro schemas."""

import random
from pathlib import Path

SCHEMA_DIR = Path(__file__).parent / "schemas"
SCHEMA_VERSIONS = ("v1", "v2")
EVENT_TYPES = ("order_created", "order_paid", "order_cancelled")
CURRENCIES = ("EUR", "USD", "GBP")
COUPONS = (None, "SPRING10", "VIP20")


def load_schema(version: str) -> str:
    if version not in SCHEMA_VERSIONS:
        raise ValueError(
            f"unknown schema version {version!r}; expected one of {SCHEMA_VERSIONS}"
        )
    return (SCHEMA_DIR / f"order_event_{version}.avsc").read_text()


def make_event_for(version: str, seq: int, rng: random.Random, now_ms: int) -> dict:
    if version not in SCHEMA_VERSIONS:
        raise ValueError(
            f"unknown schema version {version!r}; expected one of {SCHEMA_VERSIONS}"
        )
    event = {
        "order_id": f"o-{seq:09d}",
        "customer_id": f"c-{rng.randint(1, 500):04d}",
        "event_type": rng.choice(EVENT_TYPES),
        "amount_cents": rng.randint(100, 50_000),
        "currency": rng.choice(CURRENCIES),
        "event_ts_ms": now_ms,
    }
    if version == "v2":
        event["coupon_code"] = rng.choice(COUPONS)
    return event
