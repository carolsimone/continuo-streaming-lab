"""Check the record batches a broker returned (rawfetch.py --records output) against a per-record reference.

Usage: python3 fetch_vs_table.py <rawfetch JSON lines> <reference JSON lines> <partition> [<original JSON lines>]
  reference JSON lines: {"p": .., "o": <offset>, "key": .., "base": .., "byte": ..}, either the Iceberg table
    (base = kafka.batch_base_offset, byte = kafka.batch_byte_offset) or the original segment through dump_to_jsonl.py
    (base = the batch's baseOffset, byte = its position in the segment file)
  original JSON lines (optional): the original segment (dump_to_jsonl.py), to check each returned batch against the
    producer's batches
For every returned batch the true offsets of its records are looked up in the reference by key (the lab's keys are
unique order_ids). Reported: whether the broker's offsets (baseOffset + offsetDelta) equal the reference's, the
shift between them, and how the batch lines up with the reference's `base` and `byte` columns. Standard library
only. One JSON line goes to stdout, a summary to stderr.
"""

from __future__ import annotations

import json
import logging
import sys

log = logging.getLogger("fetch_vs_table")


def load_jsonl(path: str) -> list[dict]:
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip().startswith("{")]


def check(fetches: list[dict], ref_rows: list[dict], partition: int, original: list[dict] | None = None) -> dict:
    rows = sorted((r for r in ref_rows if int(r["p"]) == partition), key=lambda r: int(r["o"]))
    by_key = {r["key"]: r for r in rows}
    group_size: dict[int, int] = {}
    for r in rows:
        group_size[r["byte"]] = group_size.get(r["byte"], 0) + 1
    orig = {int(r["o"]): r for r in (original or []) if int(r["p"]) == partition}
    batches = [b for f in fetches for b in f["batches"]]
    report = []
    for b in batches:
        recs = b.get("records", [])
        true = [int(by_key[r["key"]]["o"]) if r["key"] in by_key else None for r in recs]
        first_row = by_key.get(recs[0]["key"]) if recs else None
        bytes_in_batch = {by_key[r["key"]]["byte"] for r in recs if r["key"] in by_key}
        entry = {
            "base_offset": b["base_offset"],
            "last_offset": b["last_offset"],
            "count": b["record_count"],
            "true_first": true[0] if true else None,
            "true_last": true[-1] if true else None,
            "true_contiguous": all(t is not None for t in true) and true == list(range(true[0], true[0] + len(true))),
            "offsets_correct": [r["offset"] for r in recs] == true,
            "shift": (true[0] - b["base_offset"]) if true and true[0] is not None else None,
            "base_equals_first_row_base": first_row is not None and int(first_row["base"]) == b["base_offset"],
            "one_byte_group": len(bytes_in_batch) == 1 and group_size[next(iter(bytes_in_batch))] == len(recs),
        }
        if orig and true and true[0] in orig:
            o = orig[true[0]]
            entry["first_is_last_of_original_batch"] = true[0] == o["base"] + o["count"] - 1
            entry["equals_an_original_batch"] = true[0] == o["base"] and len(recs) == o["count"]
        report.append(entry)
    covered = {t["true_first"] + i for t in report if t["true_first"] is not None for i in range(t["count"])}
    summary = {
        "partition": partition,
        "reference_rows": len(rows),
        "batches": len(report),
        "records_in_batches": sum(t["count"] for t in report),
        "reference_offsets_not_returned": len(rows) - len(covered),
        "batches_with_correct_offsets": sum(t["offsets_correct"] for t in report),
        "batches_shifted": sum(1 for t in report if t["shift"]),
        "max_shift": max((t["shift"] or 0) for t in report) if report else None,
        "batches_true_contiguous": sum(t["true_contiguous"] for t in report),
        "batches_base_equals_first_row_base": sum(t["base_equals_first_row_base"] for t in report),
        "batches_equal_one_byte_group": sum(t["one_byte_group"] for t in report),
        "overlapping_neighbours": sum(1 for a, b in zip(batches, batches[1:]) if b["base_offset"] <= a["last_offset"]),
        "gaps_between_neighbours": sum(1 for a, b in zip(batches, batches[1:]) if b["base_offset"] > a["last_offset"] + 1),
        "last_returned_offset": batches[-1]["last_offset"] if batches else None,
        "first_batches": report[:6],
    }
    if orig:
        summary["original_batches"] = len({r["base"] for r in orig.values()})
        summary["batches_starting_at_last_record_of_an_original_batch"] = sum(
            bool(t.get("first_is_last_of_original_batch")) for t in report)
        summary["batches_equal_to_an_original_batch"] = sum(bool(t.get("equals_an_original_batch")) for t in report)
    return summary


def main(argv: list[str]) -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(message)s")
    original = load_jsonl(argv[4]) if len(argv) > 4 else None
    s = check(load_jsonl(argv[1]), load_jsonl(argv[2]), int(argv[3]), original)
    log.info(
        "partition %(partition)d: %(batches)d batches, %(records_in_batches)d records (reference %(reference_rows)d "
        "rows, %(reference_offsets_not_returned)d never returned) | batches whose broker offsets equal the "
        "reference's: %(batches_with_correct_offsets)d, shifted: %(batches_shifted)d (max shift %(max_shift)s) | "
        "true offsets contiguous in batch: %(batches_true_contiguous)d | baseOffset == reference 'base' of the "
        "batch's first record: %(batches_base_equals_first_row_base)d | batch == exactly one reference 'byte' "
        "group: %(batches_equal_one_byte_group)d | overlaps with previous batch: %(overlapping_neighbours)d, gaps: "
        "%(gaps_between_neighbours)d | last offset returned %(last_returned_offset)s", s,
    )
    if original:
        log.info(
            "  against the original segment (%(original_batches)d producer batches): returned batches equal to a "
            "producer batch: %(batches_equal_to_an_original_batch)d | returned batches whose first record is the last "
            "record of a producer batch: %(batches_starting_at_last_record_of_an_original_batch)d", s,
        )
    print(json.dumps(s))  # machine-parsed stdout protocol
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
