import json
import threading
import time

import pytest
from confluent_kafka.serialization import StringSerializer

from lab import produce
from lab.produce import main, parse_args


def test_defaults():
    args = parse_args([])
    assert (args.count, args.rate, args.schema_version) == (20000, 0.0, "v1")
    assert args.topic == "events"
    assert args.bootstrap == "localhost:9092,localhost:9192,localhost:9292"
    assert args.registry == "http://localhost:8081"


def test_unreachable_registry_exits_1_quickly(capsys):
    started = time.monotonic()
    code = main(["--count", "5", "--registry", "http://127.0.0.1:1"])
    assert code == 1
    assert time.monotonic() - started < 30  # the registry client retries a few times with backoff
    assert capsys.readouterr().out == ""  # stdout is reserved for the JSON summary


def test_unknown_schema_version_exits_1():
    assert main(["--count", "1", "--schema-version", "v9"]) == 1


class FakeProducer:
    """Stands in for SerializingProducer: delivers every message successfully."""

    def __init__(self, stop=None, stop_after=None):
        self.sent = []
        self._callbacks = []
        self._stop = stop
        self._stop_after = stop_after

    def produce(self, topic, key, value, on_delivery):
        self.sent.append((topic, key, value))
        self._callbacks.append(on_delivery)
        if self._stop is not None and len(self.sent) == self._stop_after:
            self._stop.set()

    def poll(self, timeout):
        self._deliver()

    def flush(self, timeout):
        self._deliver()
        return 0

    def _deliver(self):
        callbacks, self._callbacks = self._callbacks, []
        for cb in callbacks:
            cb(None, object())


def test_count_zero_runs_until_stop_flag_then_summarises(monkeypatch):
    stop = threading.Event()
    fake = FakeProducer(stop=stop, stop_after=7)
    monkeypatch.setattr(produce, "build_producer", lambda *a, **k: fake)

    args = parse_args(["--count", "0", "--start-seq", "100"])
    summary = produce.run(args, stop)

    assert summary == {"produced": 7, "failed": 0, "first_seq": 100, "last_seq": 106}
    assert [v["order_id"] for _, _, v in fake.sent] == [f"o-{n:09d}" for n in range(100, 107)]


def test_count_zero_with_stop_already_set_sends_nothing(monkeypatch):
    stop = threading.Event()
    stop.set()
    fake = FakeProducer()
    monkeypatch.setattr(produce, "build_producer", lambda *a, **k: fake)

    summary = produce.run(parse_args(["--count", "0"]), stop)

    assert fake.sent == []
    assert summary["produced"] == 0 and summary["failed"] == 0


def test_main_count_zero_prints_one_json_line_after_stop(monkeypatch, capsys):
    stop = threading.Event()
    fake = FakeProducer(stop=stop, stop_after=3)
    monkeypatch.setattr(produce, "build_producer", lambda *a, **k: fake)
    monkeypatch.setattr(produce, "_new_stop_event", lambda: stop)

    assert main(["--count", "0"]) == 0

    out = capsys.readouterr().out.strip().splitlines()
    assert len(out) == 1
    assert json.loads(out[0]) == {"produced": 3, "failed": 0, "first_seq": 0, "last_seq": 2}


def test_finite_count_stops_at_count(monkeypatch):
    fake = FakeProducer()
    monkeypatch.setattr(produce, "build_producer", lambda *a, **k: fake)

    summary = produce.run(parse_args(["--count", "4"]), threading.Event())

    assert summary == {"produced": 4, "failed": 0, "first_seq": 0, "last_seq": 3}


class FullQueueProducer(FakeProducer):
    """Local queue is full (brokers down): produce always raises BufferError."""

    def __init__(self, stop, stop_after_polls):
        super().__init__()
        self.polls = 0
        self.flush_timeouts = []
        self._stop_event = stop
        self._stop_after_polls = stop_after_polls

    def produce(self, topic, key, value, on_delivery):
        raise BufferError("Local: Queue full")

    def poll(self, timeout):
        self.polls += 1
        if self.polls == self._stop_after_polls:
            self._stop_event.set()  # stands in for a SIGTERM arriving mid-retry

    def flush(self, timeout):
        self.flush_timeouts.append(timeout)
        return 0


def test_send_returns_when_stop_is_set_while_queue_is_full():
    stop = threading.Event()
    fake = FullQueueProducer(stop, stop_after_polls=3)

    result = produce.send(fake, "events", {"order_id": "o-1"}, lambda err, msg: None, stop)

    assert result is False
    assert fake.polls == 3  # gave up right after the stop request instead of spinning forever


def test_send_without_stop_keeps_old_signature_and_returns_true():
    fake = FakeProducer()
    assert produce.send(fake, "events", {"order_id": "o-1"}, lambda err, msg: None) is True


def test_run_with_full_queue_stops_cleanly_and_does_not_count_unsent_event(monkeypatch):
    stop = threading.Event()
    fake = FullQueueProducer(stop, stop_after_polls=2)
    monkeypatch.setattr(produce, "build_producer", lambda *a, **k: fake)

    summary = produce.run(parse_args(["--count", "0", "--start-seq", "10"]), stop)

    assert summary == {"produced": 0, "failed": 0, "first_seq": 10, "last_seq": 9}
    assert fake.flush_timeouts == [produce.STOP_FLUSH_TIMEOUT_S]


class UndeliverableProducer(FakeProducer):
    """Accepts messages but never delivers them; flush reports them as remaining."""

    def __init__(self):
        super().__init__()
        self.flush_timeouts = []

    def poll(self, timeout):
        pass

    def flush(self, timeout):
        self.flush_timeouts.append(timeout)
        return len(self.sent)


def test_stop_path_flush_timeout_is_under_compose_grace_period(monkeypatch):
    stop = threading.Event()
    fake = UndeliverableProducer()
    fake._stop = stop
    fake._stop_after = 4
    monkeypatch.setattr(produce, "build_producer", lambda *a, **k: fake)

    summary = produce.run(parse_args(["--count", "0"]), stop)

    assert fake.flush_timeouts == [produce.STOP_FLUSH_TIMEOUT_S]
    assert produce.STOP_FLUSH_TIMEOUT_S <= 10  # compose.yml stop_grace_period is 15s
    assert summary == {"produced": 0, "failed": 4, "first_seq": 0, "last_seq": 3}


def test_finite_run_keeps_long_flush_timeout(monkeypatch):
    fake = UndeliverableProducer()
    monkeypatch.setattr(produce, "build_producer", lambda *a, **k: fake)

    produce.run(parse_args(["--count", "2"]), threading.Event())

    assert fake.flush_timeouts == [produce.FLUSH_TIMEOUT_S]


def test_main_exits_1_but_still_prints_json_when_stop_flush_times_out(monkeypatch, capsys):
    stop = threading.Event()
    fake = UndeliverableProducer()
    fake._stop = stop
    fake._stop_after = 2
    monkeypatch.setattr(produce, "build_producer", lambda *a, **k: fake)
    monkeypatch.setattr(produce, "_new_stop_event", lambda: stop)

    assert main(["--count", "0"]) == 1

    out = capsys.readouterr().out.strip().splitlines()
    assert len(out) == 1
    assert json.loads(out[0]) == {"produced": 0, "failed": 2, "first_seq": 0, "last_seq": 1}


def test_build_producer_is_idempotent(monkeypatch):
    captured = {}

    class FakeRegistry:
        def __init__(self, conf):
            pass

        def get_subjects(self):
            return []

    class FakeSerializingProducer:
        def __init__(self, conf):
            captured.update(conf)

    monkeypatch.setattr(produce, "SchemaRegistryClient", FakeRegistry)
    monkeypatch.setattr(produce, "AvroSerializer", lambda registry, schema: object())
    monkeypatch.setattr(produce, "SerializingProducer", FakeSerializingProducer)
    produce.build_producer("localhost:9092", "http://localhost:8081", "{}")
    assert captured["enable.idempotence"] is True
    assert captured["bootstrap.servers"] == "localhost:9092"


def test_key_flag_defaults(monkeypatch):
    monkeypatch.delenv("LAB_KEY_SERDE", raising=False)
    monkeypatch.delenv("LAB_KEY_FIELD", raising=False)
    args = parse_args([])
    assert (args.key_serde, args.key_field) == ("string", "order_id")


def test_key_flags_from_command_line():
    args = parse_args(["--key-serde", "avro", "--key-field", "customer_id"])
    assert (args.key_serde, args.key_field) == ("avro", "customer_id")


def test_key_flag_defaults_come_from_environment(monkeypatch):
    monkeypatch.setenv("LAB_KEY_SERDE", "avro")
    monkeypatch.setenv("LAB_KEY_FIELD", "customer_id")
    args = parse_args([])
    assert (args.key_serde, args.key_field) == ("avro", "customer_id")
    args = parse_args(["--key-serde", "string", "--key-field", "order_id"])  # the flag beats the environment
    assert (args.key_serde, args.key_field) == ("string", "order_id")


def test_empty_key_environment_means_default(monkeypatch):
    # `make` and compose pass an empty string when the variable is unset.
    monkeypatch.setenv("LAB_KEY_SERDE", "")
    monkeypatch.setenv("LAB_KEY_FIELD", "")
    args = parse_args([])
    assert (args.key_serde, args.key_field) == ("string", "order_id")


def test_invalid_key_values_are_rejected(monkeypatch):
    with pytest.raises(SystemExit):
        parse_args(["--key-serde", "json"])
    with pytest.raises(SystemExit):
        parse_args(["--key-field", "amount"])
    monkeypatch.setenv("LAB_KEY_SERDE", "protobuf")  # a bad environment default is rejected too
    with pytest.raises(SystemExit):
        parse_args([])


def test_send_uses_order_id_as_key_by_default():
    fake = FakeProducer()
    event = {"order_id": "o-1", "customer_id": "c-9"}
    assert produce.send(fake, "events", event, lambda err, msg: None) is True
    assert fake.sent[0][1] == "o-1"


def test_send_uses_the_requested_key_field():
    fake = FakeProducer()
    event = {"order_id": "o-1", "customer_id": "c-9"}
    produce.send(fake, "events", event, lambda err, msg: None, key_field="customer_id")
    assert fake.sent[0][1] == "c-9"


def test_run_keys_events_by_customer_id_and_customer_keys_repeat(monkeypatch):
    fake = FakeProducer()
    monkeypatch.setattr(produce, "build_producer", lambda *a, **k: fake)

    produce.run(parse_args(["--count", "200", "--key-field", "customer_id"]), threading.Event())

    assert all(key == value["customer_id"] for _, key, value in fake.sent)
    assert len({key for _, key, _ in fake.sent}) < 200  # the same key is produced many times


def test_run_passes_key_serde_to_build_producer(monkeypatch):
    seen = {}

    def fake_build(bootstrap, registry, schema_str, key_serde="string"):
        seen["key_serde"] = key_serde
        return FakeProducer()

    monkeypatch.setattr(produce, "build_producer", fake_build)
    produce.run(parse_args(["--count", "1", "--key-serde", "avro"]), threading.Event())
    assert seen["key_serde"] == "avro"


def _build_with_fakes(monkeypatch, key_serde):
    captured = {}
    serializers = []

    class FakeRegistry:
        def __init__(self, conf):
            pass

        def get_subjects(self):
            return []

    class FakeSerializingProducer:
        def __init__(self, conf):
            captured.update(conf)

    def fake_avro(registry, schema):
        serializers.append(schema)
        return ("avro", schema)

    monkeypatch.setattr(produce, "SchemaRegistryClient", FakeRegistry)
    monkeypatch.setattr(produce, "AvroSerializer", fake_avro)
    monkeypatch.setattr(produce, "SerializingProducer", FakeSerializingProducer)
    produce.build_producer("localhost:9092", "http://localhost:8081", '{"type": "record"}', key_serde)
    return captured, serializers


def test_build_producer_string_key_serde_is_the_default(monkeypatch):
    captured, serializers = _build_with_fakes(monkeypatch, "string")
    assert isinstance(captured["key.serializer"], StringSerializer)
    assert serializers == ['{"type": "record"}']  # only the value is Avro


def test_build_producer_avro_key_serde_registers_a_string_key_schema(monkeypatch):
    captured, serializers = _build_with_fakes(monkeypatch, "avro")
    assert captured["key.serializer"] == ("avro", '{"type": "string"}')
    assert captured["value.serializer"] == ("avro", '{"type": "record"}')
    assert serializers == ['{"type": "string"}', '{"type": "record"}']
