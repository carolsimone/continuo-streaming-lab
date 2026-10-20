"""Compare what Kafka's own kafka-console-consumer.sh delivered with a reference: the Iceberg table, or the original
segment (kafka-dump-log.sh output through dump_to_jsonl.py).

Usage: python3 cc_compare.py <console-consumer output> <reference JSON lines>
  console consumer output: lines "Partition:<p>\tOffset:<o>\t<key bytes>" (print.partition, print.offset,
    print.key on, print.value off); the key is Confluent-framed Avro (magic 0, 4-byte schema id, Avro string).
  reference JSON lines: {"o": <offset>, "key": "<key>"} for one partition (`make query` or dump_to_jsonl.py).
Standard library only; no Kafka client library is involved. One JSON result line goes to stdout, a readable
summary to stderr.
"""

from __future__ import annotations

import json
import logging
import sys

log = logging.getLogger("cc_compare")


def zigzag_varint(buf: bytes, pos: int) -> tuple[int, int]:
    shift = result = 0
    while True:
        b = buf[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if not b & 0x80:
            return (result >> 1) ^ -(result & 1), pos
        shift += 7


def avro_string_key(raw: bytes) -> str:
    """Decode a Confluent-framed Avro string key: 0x00, schema id (4 bytes), zigzag length, UTF-8 bytes."""
    if len(raw) < 6 or raw[0] != 0:
        return "?" + raw.hex()
    length, pos = zigzag_varint(raw, 5)
    return raw[pos:pos + length].decode("utf-8", "replace")


def read_console(path: str) -> list[tuple[int, int, str]]:
    rows = []
    with open(path, "rb") as f:
        for line in f.read().split(b"\n"):
            if not line.startswith(b"Partition:"):
                continue
            part, off, key = line.split(b"\t", 2)
            rows.append((int(part.split(b":")[1]), int(off.split(b":")[1]), avro_string_key(key)))
    return rows


def read_reference(path: str) -> dict[int, str]:
    table = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line.startswith("{"):
                row = json.loads(line)
                table[int(row["o"])] = row["key"]
    return table


def compare(consumed: list[tuple[int, int, str]], ref: dict[int, str]) -> dict:
    offsets = [o for _, o, _ in consumed]
    keys = [k for _, _, k in consumed]
    steps_not_plus_one = [(a, b) for a, b in zip(offsets, offsets[1:]) if b != a + 1]
    seen: dict[int, int] = {}
    for o in offsets:
        seen[o] = seen.get(o, 0) + 1
    first, last = (offsets[0], offsets[-1]) if offsets else (None, None)
    missing = [o for o in range(first, last + 1) if o not in seen] if offsets else []
    key_differs = [(o, k, ref.get(o)) for _, o, k in consumed if o in ref and ref[o] != k]
    not_in_reference = [o for o in offsets if o not in ref]
    # order_id carries the producer's sequence number; in a partition the log order is increasing.
    keys_not_increasing = sum(1 for a, b in zip(keys, keys[1:]) if b <= a)
    return {
        "consumed": len(offsets),
        "first_offset": first,
        "last_offset": last,
        "steps_not_plus_one": len(steps_not_plus_one),
        "first_bad_steps": steps_not_plus_one[:8],
        "repeated_offsets": sum(n - 1 for n in seen.values() if n > 1),
        "missing_offsets_in_span": len(missing),
        "first_missing": missing[:8],
        "distinct_keys": len(set(keys)),
        "keys_not_increasing": keys_not_increasing,
        "offsets_not_in_reference": len(not_in_reference),
        "key_differs_from_reference": len(key_differs),
        "first_key_differences": key_differs[:5],
    }


def main(argv: list[str]) -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(message)s")
    result = compare(read_console(argv[1]), read_reference(argv[2]))
    log.info(
        "consumed %(consumed)d records, offsets %(first_offset)s..%(last_offset)s | steps != +1: %(steps_not_plus_one)d"
        " | repeated offsets: %(repeated_offsets)d | missing offsets in span: %(missing_offsets_in_span)d"
        " | distinct keys: %(distinct_keys)d | keys not increasing: %(keys_not_increasing)d"
        " | key differs from the reference at the same offset: %(key_differs_from_reference)d", result,
    )
    print(json.dumps(result))  # machine-parsed stdout protocol
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
