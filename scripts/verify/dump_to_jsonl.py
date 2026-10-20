"""Turn `kafka-dump-log.sh --print-data-log` output for one partition into JSON lines, one per record.

Usage: python3 dump_to_jsonl.py <partition> < dump.txt > rows.jsonl
Each line: {"p": partition, "o": offset, "key": order_id from the record key, "base": the batch's baseOffset,
"byte": the batch's position in the segment file, "count": records in the batch, "base_seq": baseSequence}.
These are the same field names fetch_vs_table.py and rows_compare.py read from the table export, so the original
on-disk segment can stand in for the table. Standard library only; a one-line summary goes to stderr.
"""

from __future__ import annotations

import json
import logging
import re
import sys

log = logging.getLogger("dump_to_jsonl")

BATCH = re.compile(r"^baseOffset: (\d+) lastOffset: (\d+) count: (\d+) baseSequence: (-?\d+) .* position: (\d+) ")
RECORD = re.compile(r"^\| offset: (\d+) .* key: .*?(o-\d{9})")


def convert(lines, partition: int) -> list[dict]:
    rows, batch = [], None
    for line in lines:
        m = BATCH.match(line)
        if m:
            batch = {"base": int(m[1]), "last": int(m[2]), "count": int(m[3]), "base_seq": int(m[4]), "byte": int(m[5])}
            continue
        m = RECORD.match(line)
        if m and batch is not None:
            rows.append({"p": partition, "o": int(m[1]), "key": m[2], "base": batch["base"], "byte": batch["byte"],
                         "count": batch["count"], "base_seq": batch["base_seq"]})
    return rows


def main(argv: list[str]) -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(message)s")
    rows = convert(sys.stdin, int(argv[1]))
    for row in rows:
        print(json.dumps(row))  # machine-parsed stdout protocol
    batches = len({r["base"] for r in rows})
    log.info("dump: %d records in %d batches, offsets %s..%s", len(rows), batches,
             rows[0]["o"] if rows else None, rows[-1]["o"] if rows else None)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
