"""Compare what Kafka's own kafka-console-consumer.sh delivered with a reference: the Iceberg table, or the original
segment (kafka-dump-log.sh output through dump_to_jsonl.py).

Usage: python3 cc_compare.py <console-consumer output> <reference JSON lines> [--id key|order_id]
  console consumer output: lines "Partition:<p>\tOffset:<o>\t<key>[\t<value>]" (print.partition, print.offset,
    print.key on; print.value optional). With key.deserializer and value.deserializer set to Kafka's
    org.apache.kafka.common.serialization.BytesDeserializer, non-printable bytes come out as \\xNN, so binary Avro
    cannot break a line; raw bytes (the default ByteArrayDeserializer) are read as well.
  reference JSON lines: {"o": <offset>, "key": "<key>", "order_id": "<order_id>"} for one partition (`make query`
    or dump_to_jsonl.py).
  --id: what names a record. `key` works when keys are unique order_ids; `order_id` (taken from the value, needs
    print.value) also works when keys repeat (customer_id keys).
Standard library only; no Kafka client library is involved. One JSON result line goes to stdout, a readable
summary to stderr.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys

log = logging.getLogger("cc_compare")
KEY_ID = re.compile(r"[oc]-\d{4,9}")  # "o-<9 digits>" (order_id keys) or "c-<4 digits>" (customer_id keys)
ORDER_ID = re.compile(r"o-\d{9}")  # order_id is the value's first field, so its first match


def read_console(path: str, id_field: str) -> list[tuple[int, int, str | None]]:
    rows = []
    with open(path, "rb") as f:
        for raw in f.read().split(b"\n"):
            line = raw.decode("latin-1")
            if not line.startswith("Partition:"):
                continue
            fields = line.split("\t", 3)
            key = KEY_ID.search(fields[2]) if len(fields) > 2 else None
            order_id = ORDER_ID.search(fields[3]) if len(fields) > 3 else None
            ident = key if id_field == "key" else order_id
            rows.append((int(fields[0].split(":")[1]), int(fields[1].split(":")[1]), ident[0] if ident else None))
    return rows


def read_reference(path: str, id_field: str) -> dict[int, str]:
    ref = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line.startswith("{"):
                row = json.loads(line)
                ref[int(row["o"])] = row.get(id_field)
    return ref


def compare(consumed: list[tuple[int, int, str | None]], ref: dict[int, str]) -> dict:
    offsets = [o for _, o, _ in consumed]
    ids = [i for _, _, i in consumed]
    steps_not_plus_one = [(a, b) for a, b in zip(offsets, offsets[1:]) if b != a + 1]
    seen: dict[int, int] = {}
    for o in offsets:
        seen[o] = seen.get(o, 0) + 1
    first, last = (offsets[0], offsets[-1]) if offsets else (None, None)
    missing = [o for o in range(first, last + 1) if o not in seen] if offsets else []
    id_differs = [(o, i, ref.get(o)) for _, o, i in consumed if o in ref and ref[o] != i]
    not_in_reference = [o for o in offsets if o not in ref]
    # order_ids carry the producer's sequence number, so along a partition's log they increase.
    ids_not_increasing = sum(1 for a, b in zip(ids, ids[1:]) if a is None or b is None or b <= a)
    return {
        "consumed": len(offsets),
        "first_offset": first,
        "last_offset": last,
        "steps_not_plus_one": len(steps_not_plus_one),
        "first_bad_steps": steps_not_plus_one[:8],
        "repeated_offsets": sum(n - 1 for n in seen.values() if n > 1),
        "missing_offsets_in_span": len(missing),
        "first_missing": missing[:8],
        "records_without_id": sum(1 for i in ids if i is None),
        "distinct_ids": len({i for i in ids if i is not None}),
        "ids_not_increasing": ids_not_increasing,
        "offsets_not_in_reference": len(not_in_reference),
        "id_differs_from_reference": len(id_differs),
        "first_id_differences": id_differs[:5],
    }


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(message)s")
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("console")
    p.add_argument("reference")
    p.add_argument("--id", choices=("key", "order_id"), default="key")
    args = p.parse_args(argv)
    result = compare(read_console(args.console, args.id), read_reference(args.reference, args.id))
    result["id"] = args.id
    log.info(
        "consumed %(consumed)d records, offsets %(first_offset)s..%(last_offset)s | steps != +1: %(steps_not_plus_one)d"
        " | repeated offsets: %(repeated_offsets)d | missing offsets in span: %(missing_offsets_in_span)d"
        " | distinct %(id)ss: %(distinct_ids)d | %(id)ss not increasing: %(ids_not_increasing)d"
        " | %(id)s differs from the reference at the same offset: %(id_differs_from_reference)d", result,
    )
    print(json.dumps(result))  # machine-parsed stdout protocol
    return 0


if __name__ == "__main__":
    sys.exit(main())
