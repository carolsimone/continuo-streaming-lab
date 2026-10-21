"""Check the record batches a broker returned (rawfetch.py --records output) against a per-record reference.

Usage: python3 fetch_vs_table.py <rawfetch JSON lines> <reference JSON lines> <partition> [<original JSON lines>]
                                 [--id key|order_id]
  reference JSON lines: {"p": .., "o": <offset>, "key": .., "order_id": .., "base": .., "byte": ..}, either the
    Iceberg table (base = kafka.batch_base_offset, byte = kafka.batch_byte_offset) or the original segment through
    dump_to_jsonl.py (base = the batch's baseOffset, byte = its position in the segment file)
  original JSON lines (optional): the original segment (dump_to_jsonl.py), to check each returned batch against the
    producer's batches
  --id: what names a record (default key; order_id, from the value, also works when keys repeat)
For every returned batch the true offsets of its records are looked up in the reference by that id. Reported: whether
the broker's offsets (baseOffset + offsetDelta) equal the reference's, the shift between them, how the batch lines up
with the reference's `base` and `byte` columns, and over ALL returned batches: CRC32C results, offsetDelta == record
index, and the offset labels the broker used (repeated, and offsets in [0, high watermark) never used). Standard
library only. One JSON line goes to stdout, a summary to stderr.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

log = logging.getLogger("fetch_vs_table")


def load_jsonl(path: str) -> list[dict]:
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip().startswith("{")]


def label_counts(fetches: list[dict]) -> dict:
    """Over every returned batch: CRC and offsetDelta checks, and the offsets the broker put on the records."""
    batches = [b for f in fetches for b in f["batches"]]
    labels = [r["offset"] for b in batches for r in b.get("records", [])]
    high = max((f["high_watermark"] for f in fetches), default=0)
    used = {o for o in labels if 0 <= o < high}
    return {
        "batches_total": len(batches),
        "batches_crc_ok": sum(1 for b in batches if b["crc_ok"]),
        "batches_offset_deltas_are_index": sum(1 for b in batches if b.get("offset_deltas_are_index")),
        "batches_compressed": sum(1 for b in batches if b["compression"] != 0),
        "records_returned": len(labels),
        "labels_repeated": len(labels) - len(set(labels)),
        "labels_unused_below_high_watermark": high - len(used),
        "high_watermark": high,
    }


def check(fetches: list[dict], ref_rows: list[dict], partition: int, original: list[dict] | None = None,
          id_field: str = "key") -> dict:
    rows = sorted((r for r in ref_rows if int(r["p"]) == partition), key=lambda r: int(r["o"]))
    by_id = {r[id_field]: r for r in rows}
    group_size: dict[int, int] = {}
    for r in rows:
        group_size[r["byte"]] = group_size.get(r["byte"], 0) + 1
    orig = {int(r["o"]): r for r in (original or []) if int(r["p"]) == partition}
    batches = [b for f in fetches for b in f["batches"]]
    report = []
    for b in batches:
        recs = b.get("records", [])
        true = [int(by_id[r[id_field]]["o"]) if r[id_field] in by_id else None for r in recs]
        first_row = by_id.get(recs[0][id_field]) if recs else None
        bytes_in_batch = {by_id[r[id_field]]["byte"] for r in recs if r[id_field] in by_id}
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
        "id": id_field,
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
        **label_counts(fetches),
        "first_batches": report[:6],
    }
    if orig:
        summary["original_batches"] = len({r["base"] for r in orig.values()})
        summary["batches_starting_at_last_record_of_an_original_batch"] = sum(
            bool(t.get("first_is_last_of_original_batch")) for t in report)
        summary["batches_equal_to_an_original_batch"] = sum(bool(t.get("equals_an_original_batch")) for t in report)
    return summary


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(message)s")
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("fetches")
    p.add_argument("reference")
    p.add_argument("partition", type=int)
    p.add_argument("original", nargs="?")
    p.add_argument("--id", choices=("key", "order_id"), default="key")
    args = p.parse_args(argv)
    original = load_jsonl(args.original) if args.original else None
    s = check(load_jsonl(args.fetches), load_jsonl(args.reference), args.partition, original, args.id)
    log.info(
        "partition %(partition)d: %(batches)d batches, %(records_in_batches)d records (reference %(reference_rows)d "
        "rows, %(reference_offsets_not_returned)d never returned) | batches whose broker offsets equal the "
        "reference's: %(batches_with_correct_offsets)d, shifted: %(batches_shifted)d (max shift %(max_shift)s) | "
        "true offsets contiguous in batch: %(batches_true_contiguous)d | baseOffset == reference 'base' of the "
        "batch's first record: %(batches_base_equals_first_row_base)d | batch == exactly one reference 'byte' "
        "group: %(batches_equal_one_byte_group)d | overlaps with previous batch: %(overlapping_neighbours)d, gaps: "
        "%(gaps_between_neighbours)d | last offset returned %(last_returned_offset)s", s,
    )
    log.info(
        "  all returned batches: %(batches_total)d, CRC32C ok %(batches_crc_ok)d, offsetDelta == record index "
        "%(batches_offset_deltas_are_index)d, compressed %(batches_compressed)d | offset labels on %(records_returned)d "
        "records: repeated %(labels_repeated)d, offsets in [0, %(high_watermark)d) never used "
        "%(labels_unused_below_high_watermark)d", s,
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
    sys.exit(main())
