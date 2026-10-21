"""Compare the table's kafka.batch_byte_offset with the original segment's offset index.

Usage: python3 index_vs_table.py <kafka-dump-log.sh output of the .index file> <table JSON lines> <partition>
  index dump lines: "offset: <offset> position: <byte position>"
  table JSON lines: {"p": .., "o": <offset>, "byte": <kafka.batch_byte_offset>, ...}
For every table row it computes what an offset-index lookup would return for the row's offset (the position of the
entry with the largest offset <= the row's offset, 0 if there is none) and counts rows where batch_byte_offset equals
it. Also: the runs of consecutive rows sharing one batch_byte_offset, and how many of them start exactly at an index
entry. Standard library only. One JSON line goes to stdout, a summary to stderr.
"""

from __future__ import annotations

import bisect
import json
import logging
import re
import sys

log = logging.getLogger("index_vs_table")
ENTRY = re.compile(r"^offset: (\d+) position: (\d+)")


def main(argv: list[str]) -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(message)s")
    with open(argv[1], errors="replace") as f:
        entries = sorted({(int(m[1]), int(m[2])) for m in map(ENTRY.match, f) if m})
    partition = int(argv[3])
    with open(argv[2]) as f:
        rows = sorted((r for r in map(json.loads, (line for line in f if line.startswith("{")))
                       if int(r["p"]) == partition), key=lambda r: int(r["o"]))
    keys = [o for o, _ in entries]

    def lookup(offset: int) -> int:
        i = bisect.bisect_right(keys, offset)
        return entries[i - 1][1] if i else 0

    equal = sum(1 for r in rows if int(r["byte"]) == lookup(int(r["o"])))
    runs = []
    for r in rows:
        if not runs or runs[-1][1] != r["byte"]:
            runs.append((int(r["o"]), int(r["byte"])))
    entry_set = set(entries)
    result = {
        "index_entries": len(entries),
        "first_entries": entries[:5],
        "table_rows": len(rows),
        "rows_where_byte_equals_index_lookup": equal,
        "byte_runs": len(runs),
        "runs_starting_at_an_index_entry": sum(1 for run in runs if run in entry_set),
        "first_runs": runs[:5],
    }
    log.info("partition %d: %d index entries, %d table rows; batch_byte_offset == offset-index lookup(offset) on %d rows"
             " | %d runs of equal batch_byte_offset, %d of them start at an index entry (offset, position)", partition,
             result["index_entries"], result["table_rows"], equal, result["byte_runs"],
             result["runs_starting_at_an_index_entry"])
    print(json.dumps(result))  # machine-parsed stdout protocol
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
