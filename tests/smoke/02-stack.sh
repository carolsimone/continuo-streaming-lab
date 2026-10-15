#!/usr/bin/env bash
source "$(dirname "$0")/../../scripts/lib.sh"

make clean
make up

for n in 1 2 3; do
  # Capture the log first: with pipefail, `grep -q` closing the pipe early would SIGPIPE docker compose.
  broker_log="$(docker compose logs "kafka-${n}" 2>&1)"
  if grep -E 'NoClassDefFoundError|ClassNotFoundException' <<<"${broker_log}"; then
    fail "plugin class path problem in kafka-${n} log"
  fi
  grep -q 'RemoteLogManager' <<<"${broker_log}" \
    || fail "kafka-${n} never started the RemoteLogManager (tiered storage not active)"
done

port="${SCHEMA_REGISTRY_PORT:-8081}"
curl -fs "http://localhost:${port}/subjects" >/dev/null || fail "Karapace not answering"

describe="$(make -s topic-describe)"
echo "${describe}" | grep -q 'PartitionCount: 3' || fail "events does not have 3 partitions"
echo "${describe}" | grep -q 'remote.storage.enable=true' || fail "tiering not enabled on events"
leaders="$(echo "${describe}" | grep -oE 'Leader: [0-9]+' | sort -u | wc -l)"
(( leaders >= 2 )) || log "NOTE: all partition leaders sit on one broker (${leaders} distinct)"
echo "${describe}" | grep 'Partition:' | grep -vq 'Isr: [0-9],[0-9],[0-9]' && fail "a partition does not have a full ISR"
log "PASS: 3 brokers with plugin, Karapace, and a tiered 3-partition topic are up"
