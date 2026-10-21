"""Compare two per-record exports of one partition offset by offset (for example the original segment, through
dump_to_jsonl.py, against the Iceberg table), field by field.

Usage: python3 rows_compare.py <reference JSON lines> <other JSON lines> <partition> [field ...]
  default fields: key base byte (record key, batch base offset, batch byte position)
For every field: offsets where both sides agree, offsets where they differ, and the first differences.
Standard library only. One JSON line goes to stdout, a summary to stderr.
"""

from __future__ import annotations

import json
import logging
import sys

log = logging.getLogger("rows_compare")


def load(path: str, partition: int) -> dict[int, dict]:
    with open(path) as f:
        rows = [json.loads(line) for line in f if line.strip().startswith("{")]
    return {int(r["o"]): r for r in rows if int(r["p"]) == partition}


def compare(ref: dict[int, dict], other: dict[int, dict], fields: list[str]) -> dict:
    common = sorted(ref.keys() & other.keys())
    result = {"reference_rows": len(ref), "other_rows": len(other), "common_offsets": len(common),
              "only_in_reference": len(ref.keys() - other.keys()), "only_in_other": len(other.keys() - ref.keys())}
    for field in fields:
        diff = [(o, ref[o].get(field), other[o].get(field)) for o in common if ref[o].get(field) != other[o].get(field)]
        result[field] = {"equal": len(common) - len(diff), "differ": len(diff), "first_differences": diff[:6]}
    return result


def main(argv: list[str]) -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(message)s")
    partition = int(argv[3])
    fields = argv[4:] or ["key", "base", "byte"]
    r = compare(load(argv[1], partition), load(argv[2], partition), fields)
    log.info("partition %d: %d offsets in reference, %d in other, %d in both | %s", partition, r["reference_rows"],
             r["other_rows"], r["common_offsets"],
             " | ".join(f"{f}: equal {r[f]['equal']}, differ {r[f]['differ']}" for f in fields))
    print(json.dumps(r))  # machine-parsed stdout protocol
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
