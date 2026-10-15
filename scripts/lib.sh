#!/usr/bin/env bash
# Shared shell helpers. Source this file; it moves to the repo root.
# Diagnostics go to stderr so stdout stays free for machine-parsed output.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

log()  { echo "[lab] $*" >&2; }
fail() { log "FAIL: $*"; exit 1; }

# wait_for <seconds> <description> <command...>
wait_for() {
  local deadline=$((SECONDS + $1)) what=$2
  shift 2
  until "$@" >/dev/null 2>&1; do
    (( SECONDS < deadline )) || fail "timed out waiting for: ${what}"
    sleep 2
  done
  log "ok: ${what}"
}

# jget <key>: print <key> from the last JSON line on stdin.
jget() {
  python3 -c "import json,sys; print(json.loads(sys.stdin.read().strip().splitlines()[-1])['$1'])"
}

mc() { scripts/mc.sh "$@"; }
