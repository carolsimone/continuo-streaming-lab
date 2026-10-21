#!/usr/bin/env bash
# Helpers for the stage 2 verification scripts. Source this file (it moves to the repo root via scripts/lib.sh).
# Every script prints each command it runs ("$ ..."), then the command's real output (stdout and stderr merged).
# See scripts/verify/README.md for what each script needs and the order to run them in.
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"
export STAGE=iceberg COMPOSE_FILE=compose.yml:compose.iceberg.yml
REST="http://localhost:${ICEBERG_REST_PORT:-8181}"

# run <command...>: print the command, then its output. A non-zero exit is printed, not fatal.
run() {
  echo "\$ $*"
  local rc=0
  "$@" 2>&1 || rc=$?
  (( rc == 0 )) || echo "(exit code ${rc})"
  echo
}

# query <sql>: run SQL through `make query` (the SQL travels in the SQL environment variable).
query() {
  echo "\$ SQL=$(printf '%q' "$1") make -s query"  # %q: the printed line can be copied and re-run
  local rc=0
  SQL="$1" make -s query 2>&1 || rc=$?
  (( rc == 0 )) || echo "(exit code ${rc})"
  echo
}

# query_capture <sql>: like query, but also keeps the JSON lines (stdout only) in QUERY_OUT for later use.
query_capture() {
  echo "\$ SQL=$(printf '%q' "$1") make -s query"
  local rc=0 err
  err="$(mktemp)"
  QUERY_OUT="$(SQL="$1" make -s query 2>"${err}")" || rc=$?
  cat "${err}"
  rm -f "${err}"
  echo "${QUERY_OUT}"
  (( rc == 0 )) || echo "(exit code ${rc})"
  echo
}

# metadata_location: the table's current metadata file, as the REST catalog reports it.
metadata_location() {
  curl -fsS "${REST}/v1/namespaces/default/tables/events" |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata-location"])'
}

# note <text>: a heading line in the transcript.
note() { echo "## $*"; echo; }

# wait_for_rows <expected> [seconds]: poll the table until it holds at least <expected> rows (bounded, default 240 s).
wait_for_rows() {
  local expected=$1 deadline=$((SECONDS + ${2:-240})) rows=0 out
  until (( rows >= expected )); do
    (( SECONDS < deadline )) || fail "table holds ${rows} rows after waiting, expected at least ${expected}"
    sleep 5
    out="$(make -s query 2>/dev/null)" && rows="$(echo "${out}" | jget row_count)" || rows=0
    log "table rows: ${rows} of ${expected}"
  done
}

# ---- helpers shared by stage2-duplicates.sh and stage2-replay-source.sh ----
OFFSETS="docker compose exec -T kafka-1 /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka-1:19092 --topic events"
utc_hms() { python3 -c 'import datetime as d; print(d.datetime.now(d.timezone.utc).strftime("%H:%M:%S.%f")[:-3])'; }

# leader_of <partition>: broker id leading the partition, from kafka-topics --describe.
leader_of() {
  make -s topic-describe 2>/dev/null | grep -E "Partition: $1[[:space:]]" | sed -E 's/.*Leader: ([0-9]+).*/\1/' | tr -d '\r'
}

# wait_rows_at_least <rows> <seconds>: poll the table (non-fatal), print the last row count.
wait_rows_at_least() {
  local want=$1 deadline=$((SECONDS + $2)) rows=0 out
  until (( rows >= want )); do
    (( SECONDS < deadline )) || { log "table holds ${rows} rows after waiting, wanted ${want}"; return 1; }
    sleep 5
    out="$(make -s query 2>/dev/null)" && rows="$(jget row_count <<<"${out}")" || rows=0
  done
  log "table rows: ${rows} (wanted ${want})"
}

# end_total: sum of the broker end offsets over the partitions.
end_total() {
  ${OFFSETS} --time -1 2>/dev/null | awk -F: '{s += $3} END {print s}'
}

# pairs_check <label>: the three-way comparison, one JSON line plus a per-partition summary.
pairs_check() {
  local out rc=0 err
  err="$(mktemp)"
  echo "\$ uv run python -m lab.pairs   # stdout = one JSON line"
  out="$(uv run python -m lab.pairs 2>"${err}")" || rc=$?
  grep -E ' (INFO|ERROR) __main__' "${err}" | cut -c1-300 || true
  rm -f "${err}"
  echo "${out}"
  (( rc == 0 )) || echo "(exit code ${rc})"
  echo
  PAIRS="${out}" python3 - <<'PY' 2>&1
import json
import os

r = json.loads(os.environ["PAIRS"]) if os.environ["PAIRS"] else None
if r is None:
    print("no result")
    raise SystemExit(0)
for p, v in r["partitions"].items():
    print(f"partition {p}: broker end offset {v['end_offset']} (log start {v['low']}) | table rows {v['table_rows']}, "
          f"duplicates {v['table_duplicates']}, missing {v['table_missing']} {v['table_missing_ranges']} | "
          f"replay rows {v['replay_rows']}, duplicates {v['replay_duplicates']}, missing {v['replay_missing']} "
          f"{v['replay_missing_ranges']} | in table only {v['table_only']}, in replay only {v['replay_only']}")
t = r["totals"]
c = (r.get("content") or {}).get("totals")
if c is None:
    print(f"content: not compared (table {r.get('table_status')})")
else:
    print(f"content: offsets whose record differs between table and replay {c['offsets_with_different_record']}, "
          f"order_ids repeated in the replay {c['replay_duplicate_order_ids']}, in the table {c['table_duplicate_order_ids']}, "
          f"order_ids in the table but not in the replay {c.get('table_order_ids_not_in_replay')}, "
          f"in the replay but not in the table {c.get('replay_order_ids_not_in_table')}")
print(f"replay status: {r['replay_status']}; consumer positions at the end {r.get('replay_positions')}")
print(f"total: broker records {t['expected']}, table rows {t['table_rows']}, table duplicates {t['table_duplicates']}, "
      f"table missing {t['table_missing']}, replay rows {t['replay_rows']}, replay duplicates {t['replay_duplicates']}, "
      f"replay missing {t['replay_missing']}")
PY
  echo
}
