import json
import random

import pytest
from fastavro import parse_schema
from fastavro.validation import validate

from lab.events import SCHEMA_VERSIONS, load_schema, make_event_for


@pytest.mark.parametrize("version", SCHEMA_VERSIONS)
def test_event_validates_against_its_schema(version):
    schema = parse_schema(json.loads(load_schema(version)))
    event = make_event_for(version, 7, random.Random(1), 1_700_000_000_000)
    assert validate(event, schema)


def test_same_seed_gives_same_events():
    a = make_event_for("v1", 3, random.Random(42), 1000)
    b = make_event_for("v1", 3, random.Random(42), 1000)
    assert a == b


def test_order_id_is_unique_per_sequence_number():
    rng = random.Random(0)
    ids = {make_event_for("v1", seq, rng, 0)["order_id"] for seq in range(1000)}
    assert len(ids) == 1000


def test_v2_adds_optional_coupon_with_null_default():
    fields = {f["name"]: f for f in json.loads(load_schema("v2"))["fields"]}
    assert fields["coupon_code"]["type"] == ["null", "string"]
    assert fields["coupon_code"]["default"] is None
    assert "coupon_code" not in {f["name"] for f in json.loads(load_schema("v1"))["fields"]}


def test_unknown_version_is_rejected_with_a_clear_message():
    with pytest.raises(ValueError, match="unknown schema version 'v9'"):
        load_schema("v9")
