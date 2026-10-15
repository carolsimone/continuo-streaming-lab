import json
import threading
import time

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
