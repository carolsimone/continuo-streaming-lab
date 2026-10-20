#!/usr/bin/env bash
# Stage 2: the broker writes an Iceberg table; DuckDB reads it through the REST catalog.
# Uses the default 1 MiB segments (a small segment.bytes breaks an unthrottled batch, see
# docs/findings/02-stage2-iceberg.md). The last open segment of each partition only uploads after the
# idle roll, about 35 to 40 s after the last write, so the test polls until the table holds every record.
#
# Pass/fail: FAIL on missing records (produced minus distinct (partition, offset) pairs), on rows from
# fewer than 3 partitions, on a table that does not start at offset 0, and when kafka-tiered is not empty.
# Duplicate rows are a finding, not a failure: they are counted and printed.
source "$(dirname "$0")/../../scripts/lib.sh"
export STAGE=iceberg COMPOSE_FILE=compose.yml:compose.iceberg.yml

REST="http://localhost:${ICEBERG_REST_PORT:-8181}"
TABLE_WAIT_S=240
NO_TABLE=3  # exit status of lab.query when the table does not exist yet

# Stage 2 is reset with `make clean`, never `make down`: removing the iceberg-rest container loses
# the catalog and leaves Parquet files orphaned in the warehouse bucket.
make clean
make up STAGE=iceberg

# The REST fixture has no healthcheck: poll its config endpoint.
wait_for 120 "Iceberg REST catalog answers at ${REST}/v1/config" curl -fsS "${REST}/v1/config"

out="$(make -s batch N=20000)"
produced="$(echo "${out}" | jget produced)"
[[ "${produced}" == "20000" ]] || fail "expected 20000 produced, got ${produced}"

# table_stats: one JSON line {rows, distinct_pairs, partitions, min_offset}. Calls the tool directly instead
# of `make query`, because make reports any failed recipe as exit status 2 and hides the tool's own status.
# Status 3 means "no table yet" and yields zeros; any other failure is surfaced at once.
STATS_SQL='SELECT count(*) AS rows, count(DISTINCT (kafka.partition, kafka."offset")) AS distinct_pairs, count(DISTINCT kafka.partition) AS partitions, min(kafka."offset") AS min_offset FROM {table}'
table_stats() {
  local out status=0 err
  err="$(mktemp)"
  out="$(uv run python -m lab.query --sql "${STATS_SQL}" 2>"${err}")" || status=$?
  if (( status == NO_TABLE )); then
    echo '{"rows": 0, "distinct_pairs": 0, "partitions": 0, "min_offset": null}'
  elif (( status != 0 )); then
    log "query failed with exit status ${status}:"
    cat "${err}" >&2
    rm -f "${err}"
    exit 1
  else
    echo "${out}"
  fi
  rm -f "${err}"
}

deadline=$((SECONDS + TABLE_WAIT_S))
stats="$(table_stats)"
until (( $(echo "${stats}" | jget distinct_pairs) >= produced )); do
  (( SECONDS < deadline )) || break
  sleep 5
  stats="$(table_stats)"
  log "table rows: $(echo "${stats}" | jget rows), distinct (partition, offset) pairs: $(echo "${stats}" | jget distinct_pairs), of ${produced} produced"
done

rows="$(echo "${stats}" | jget rows)"
distinct="$(echo "${stats}" | jget distinct_pairs)"
partitions="$(echo "${stats}" | jget partitions)"
min_offset="$(echo "${stats}" | jget min_offset)"
(( distinct >= produced )) || fail "after ${TABLE_WAIT_S} s the table holds ${distinct} distinct records of ${produced} produced (missing $((produced - distinct)), rows ${rows})"
[[ "${min_offset}" == "0" ]] || fail "table starts at offset ${min_offset}, expected 0"
[[ "${partitions}" == "3" ]] || fail "rows from ${partitions} partitions, expected 3"

per_partition="$(make -s query SQL='SELECT kafka.partition AS p, count(*) AS n, count(DISTINCT kafka."offset") AS d FROM {table} GROUP BY 1 ORDER BY 1')" || fail "per-partition query failed (see the error above)"
log "per-partition rows and distinct offsets:"
echo "${per_partition}" >&2

duplicates=$(( rows - distinct ))
if (( duplicates > 0 )); then
  log "FINDING: ${duplicates} duplicate rows (${rows} rows, ${distinct} distinct (partition, offset) pairs)"
fi

# Iceberg mode writes nothing to kafka-tiered; everything lands in warehouse.
tiered="$(scripts/mc.sh ls --recursive local/kafka-tiered)"
[[ -z "${tiered}" ]] || fail "kafka-tiered is not empty in Iceberg mode: ${tiered}"
warehouse="$(scripts/mc.sh ls --recursive local/warehouse)"
grep -q '\.parquet' <<<"${warehouse}" || fail "no Parquet file in the warehouse bucket"

log "PASS: ${distinct} of ${produced} produced records queryable in Iceberg (${rows} rows, ${duplicates} duplicates, 3 partitions), kafka-tiered empty, Parquet in warehouse"
