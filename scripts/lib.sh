#!/usr/bin/env bash
# Shared shell helpers. Source this file; it moves to the repo root.
# Diagnostics go to stderr so stdout stays free for machine-parsed output.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

# Load KEY=value lines from the gitignored .env (the same file compose reads), so port overrides reach
# these scripts too. Variables already set (non-empty) in the shell win, as they do in compose.
if [[ -f .env ]]; then
  while IFS='=' read -r _key _value || [[ -n "${_key}" ]]; do
    [[ "${_key}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue
    _value="${_value%\"}"; _value="${_value#\"}"
    [[ -n "${!_key:-}" ]] || export "${_key}=${_value}"
  done < .env
  unset _key _value
fi

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
