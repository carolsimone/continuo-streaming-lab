#!/usr/bin/env bash
# Control for experiment 7: the same topic, the same leaders, replayed twice by python -m lab.pairs.
#   1) local.retention.ms=3600000: the brokers keep every segment on disk, so the replay is served from the local log.
#   2) after `kafka-configs --alter local.retention.ms=30000`, once the old local segments are gone: the replay of the
#      history is served from tiered storage, i.e. rebuilt by the broker from the Iceberg table's Parquet files.
# Each replay is compared with the table by (partition, offset) and by the record (order_id) found at that offset.
# Environment: RS_SEGMENT_BYTES (262144), RS_PACE (events/s, 2000; 0 = unthrottled), RS_BATCH (20000).
# Transcripts: stage2-replay-source-256k-paced.txt (defaults), -1m-unthrottled.txt (RS_SEGMENT_BYTES=1048576 RS_PACE=0),
# -1m-paced.txt (RS_SEGMENT_BYTES=1048576 RS_PACE=2000).
# Usage: scripts/verify/stage2-replay-source.sh > docs/findings/evidence/stage2-replay-source-256k-paced.txt 2>&1
source "$(dirname "$0")/lib.sh"

BATCH="${RS_BATCH:-20000}"
PACE="${RS_PACE:-2000}"
SEGMENT_BYTES="${RS_SEGMENT_BYTES:-262144}"  # RS_SEGMENT_BYTES=1048576 RS_PACE=0 is the lab default topic with an unthrottled batch
note "settings: segment.bytes=${SEGMENT_BYTES}, local.retention.ms=3600000 for the first replay, 30000 for the second; batch ${BATCH} at ${PACE}/s keyed by customer_id"
run make clean
run make up STAGE=iceberg
echo "\$ SEGMENT_BYTES=${SEGMENT_BYTES} LOCAL_RETENTION_MS=3600000 make topic"
SEGMENT_BYTES=${SEGMENT_BYTES} LOCAL_RETENTION_MS=3600000 make topic 2>&1 | grep -E 'Created topic|Configs:'
echo
run make topic-describe
echo "\$ make -s batch N=${BATCH} PACE=${PACE} LAB_KEY_FIELD=customer_id"
make -s batch N=${BATCH} PACE=${PACE} LAB_KEY_FIELD=customer_id 2>/dev/null
echo
wait_rows_at_least 10000 240 || true
sleep 5
note "broker end offsets and what each broker holds on disk (nothing is deleted yet)"
echo "\$ ${OFFSETS} --time -1"; ${OFFSETS} --time -1 2>&1; echo
run make ls-local

note "1) replay while every segment is still on the brokers' disks (the open segments are not in the table yet)"
pairs_check local

note "switch to local.retention.ms=30000 and wait until the oldest local segments are deleted"
run docker compose exec -T kafka-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-1:19092 --alter --entity-type topics --entity-name events --add-config local.retention.ms=30000
wait_for 480 "the first segment (base offset 0) is gone from every broker" \
  bash -c '! make -s ls-local 2>/dev/null | grep -q "00000000000000000000.log"'
wait_rows_at_least "$(end_total)" 120 || true
run make ls-local

note "2) replay of the same topic, history now served from tiered storage"
pairs_check remote
run make clean
