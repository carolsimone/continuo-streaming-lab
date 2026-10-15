#!/usr/bin/env bash
# Karapace must serve writes as soon as `make up` returns (fresh stack, no retries, no sleeps).
source "$(dirname "$0")/../../scripts/lib.sh"

make clean
make up

port="${SCHEMA_REGISTRY_PORT:-8081}"
base="http://localhost:${port}"
subject="race-probe-value"

# Register a probe schema; the deadline covers the whole attempt (a hung registry never answers).
body="$(curl -sS --max-time 30 -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  --data '{"schema":"\"string\""}' "${base}/subjects/${subject}/versions")" \
  || fail "schema registration did not complete within 30 s"
id="$(jget id <<<"${body}")" || fail "unexpected registration response: ${body}"
log "registered ${subject} with schema id ${id}"

# Clean up the probe (soft delete, then permanent) so reruns and later tests start from an empty registry.
curl -fsS --max-time 10 -X DELETE "${base}/subjects/${subject}" >/dev/null || fail "soft delete of ${subject} failed"
curl -fsS --max-time 10 -X DELETE "${base}/subjects/${subject}?permanent=true" >/dev/null \
  || fail "permanent delete of ${subject} failed"

master="$(curl -fsS --max-time 5 -X GET -H 'Content-Type: application/json' --data '{}' "${base}/master_available")"
grep -q '"master_available": *true' <<<"${master}" || fail "master_available is not true: ${master}"

log "PASS: Karapace is master and accepted a schema write right after make up"
