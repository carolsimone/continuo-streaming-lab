#!/usr/bin/env bash
# (Re)create the tiered `events` topic. Defaults match compose.yml's topic-init. Tune rolling via
# SEGMENT_BYTES / LOCAL_RETENTION_MS; segment.ms is only set when SEGMENT_MS is passed explicitly.
source "$(dirname "$0")/lib.sh"

PARTITIONS="${PARTITIONS:-3}"
REPLICATION_FACTOR="${REPLICATION_FACTOR:-3}"
SEGMENT_BYTES="${SEGMENT_BYTES:-1048576}"
SEGMENT_MS="${SEGMENT_MS:-}"
LOCAL_RETENTION_MS="${LOCAL_RETENTION_MS:-30000}"
TOPICS="docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092"

if ${TOPICS} --list | grep -qx events; then
  log "deleting existing topic events"
  ${TOPICS} --delete --topic events
  wait_for 60 "topic events deleted" bash -c "! ${TOPICS} --list | grep -qx events"
fi

${TOPICS} --create --topic events --partitions "${PARTITIONS}" --replication-factor "${REPLICATION_FACTOR}" \
  --config min.insync.replicas=2 \
  --config remote.storage.enable=true \
  --config retention.ms=-1 \
  --config segment.bytes="${SEGMENT_BYTES}" \
  ${SEGMENT_MS:+--config segment.ms="${SEGMENT_MS}"} \
  --config local.retention.ms="${LOCAL_RETENTION_MS}"
${TOPICS} --describe --topic events >&2
