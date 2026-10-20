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
  (( rc == 0 )) || echo "(exit code ${rc}; make reports the tool's exit status 3 as 2)"
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
  (( rc == 0 )) || echo "(exit code ${rc}; make reports the tool's exit status 3 as 2)"
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
