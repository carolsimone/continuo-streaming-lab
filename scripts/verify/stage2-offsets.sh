#!/usr/bin/env bash
# (v) Rows per partition and offset coverage in the table, against what the brokers hold.
# Needs the running stack with all offloaded data (run after stage2-key-partition.sh, or after the smoke test).
source "$(dirname "$0")/lib.sh"

note "per partition, from the table"
query_capture 'SELECT kafka.partition AS p, count(*) AS n, min(kafka."offset") AS lo, max(kafka."offset") AS hi, count(DISTINCT kafka."offset") AS d FROM {table} GROUP BY 1 ORDER BY 1'
table="${QUERY_OUT}"

note "per partition, from the broker: end offset (--time -1) and log start offset (--time -2)"
OFFSETS="docker compose exec -T kafka-1 /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka-1:19092 --topic events"
echo "\$ ${OFFSETS} --time -1"
ends="$(${OFFSETS} --time -1 2>/dev/null)"
echo "${ends}"
echo
echo "\$ ${OFFSETS} --time -2"
${OFFSETS} --time -2 2>&1
echo

note "comparison of the two outputs above: broker end offset against distinct offsets in the table, and offset contiguity"
ENDS="${ends}" TABLE="${table}" python3 - <<'PY' 2>&1
import json
import os

end = {}
for line in os.environ["ENDS"].splitlines():
    _topic, partition, offset = line.strip().split(":")
    end[int(partition)] = int(offset)
total_missing = total_dup = 0
for line in os.environ["TABLE"].splitlines():
    r = json.loads(line)
    p = r["p"]
    missing = end[p] - r["d"]  # offsets the broker holds that the table lacks
    duplicates = r["n"] - r["d"]  # rows repeating an offset
    contiguous = r["hi"] - r["lo"] + 1 == r["d"]
    total_missing += missing
    total_dup += duplicates
    print(
        f"partition {p}: broker end offset {end[p]}, table rows {r['n']}, distinct offsets {r['d']}, "
        f"min {r['lo']}, max {r['hi']}, missing {missing}, duplicate rows {duplicates}, contiguous {contiguous}"
    )
print(f"total: missing {total_missing}, duplicate rows {total_dup}, broker records {sum(end.values())}")
PY
echo
