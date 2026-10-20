#!/usr/bin/env bash
# Stage 2: the broker writes an Iceberg table; DuckDB reads it through the REST catalog.
# Uses the default 1 MiB segments (a small segment.bytes breaks an unthrottled batch, see
# docs/findings/02-stage2-iceberg.md). The last open segment of each partition only uploads after the
# idle roll, about 40 s after the last write, so the test polls until the table holds every record.
source "$(dirname "$0")/../../scripts/lib.sh"
export STAGE=iceberg COMPOSE_FILE=compose.yml:compose.iceberg.yml

REST="http://localhost:${ICEBERG_REST_PORT:-8181}"
TABLE_WAIT_S=240

# Stage 2 is reset with `make clean`, never `make down`: removing the iceberg-rest container loses
# the catalog and leaves Parquet files orphaned in the warehouse bucket.
make clean
make up STAGE=iceberg

# The REST fixture has no healthcheck: poll its config endpoint.
wait_for 120 "Iceberg REST catalog answers at ${REST}/v1/config" curl -fsS "${REST}/v1/config"

out="$(make -s batch N=20000)"
produced="$(echo "${out}" | jget produced)"
[[ "${produced}" == "20000" ]] || fail "expected 20000 produced, got ${produced}"

# table_rows: row count of default.events, or 0 while the table does not exist yet (query exits 3).
table_rows() {
  local out
  out="$(make -s query 2>/dev/null)" || { echo 0; return 0; }
  echo "${out}" | jget row_count
}

deadline=$((SECONDS + TABLE_WAIT_S))
rows=0
until (( rows == produced )); do
  (( SECONDS < deadline )) || break
  sleep 5
  rows="$(table_rows)"
  log "table rows: ${rows} of ${produced}"
done
(( rows == produced )) || fail "after ${TABLE_WAIT_S} s the table holds ${rows} of ${produced} produced records (missing $((produced - rows)))"

# Rows come from all 3 partitions, offsets start at 0, and (partition, offset) pairs are distinct.
summary="$(make -s query 2>/dev/null)"
min_offset="$(echo "${summary}" | jget min_offset)"
partitions="$(echo "${summary}" | jget partitions)"
[[ "${min_offset}" == "0" ]] || fail "table starts at offset ${min_offset}, expected 0"
[[ "${partitions}" == "3" ]] || fail "rows from ${partitions} partitions, expected 3"

per_partition="$(make -s query SQL='SELECT kafka.partition AS p, count(*) AS n, count(DISTINCT kafka."offset") AS d FROM {table} GROUP BY 1 ORDER BY 1' 2>/dev/null)"
log "per-partition rows and distinct offsets:"
echo "${per_partition}" >&2
duplicates="$(echo "${per_partition}" | python3 -c '
import json, sys
rows = [json.loads(line) for line in sys.stdin if line.strip()]
print(sum(r["n"] - r["d"] for r in rows))')"
if [[ "${duplicates}" != "0" ]]; then
  log "FINDING: ${duplicates} duplicate (partition, offset) rows in the table"
  fail "duplicates visible: ${duplicates} rows repeat a (partition, offset) pair"
fi

# Iceberg mode writes nothing to kafka-tiered; everything lands in warehouse.
tiered="$(scripts/mc.sh ls --recursive local/kafka-tiered)"
[[ -z "${tiered}" ]] || fail "kafka-tiered is not empty in Iceberg mode: ${tiered}"
warehouse="$(scripts/mc.sh ls --recursive local/warehouse)"
grep -q '\.parquet' <<<"${warehouse}" || fail "no Parquet file in the warehouse bucket"

log "PASS: ${rows} of ${produced} produced records queryable in Iceberg (3 partitions, no duplicate offsets), kafka-tiered empty, Parquet in warehouse"
