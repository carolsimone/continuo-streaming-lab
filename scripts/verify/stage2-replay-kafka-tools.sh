#!/usr/bin/env bash
# Task 13b: is the wrong Iceberg-mode replay (findings 03, experiment 7) real, or an artefact of lab/pairs.py and
# lab/replay.py? Checked here WITHOUT the lab's client (librdkafka via confluent_kafka):
#   a) kafka-dump-log.sh on partition 0's original segment and its offset index while they are still on the leader's
#      disk (ground truth: the producer's batches, their offsets, positions and keys), and Kafka's own
#      kafka-console-consumer.sh on the local log;
#   b) after the segment is uploaded and deleted locally: kafka-console-consumer.sh from offset 0 and from a window deep
#      in the tiered range, compared offset by offset with the Iceberg table (DuckDB);
#   c) raw Fetch requests (rawfetch.py, a TCP socket and the Kafka wire format, standard library only) to the leader of
#      each partition: every record batch the broker returns, its header offsets and its records' keys, checked
#      against the table and against the original segment (fetch_vs_table.py);
#   d) the partition-0 leader's log during those reads (kafka.log.remote at DEBUG);
#   e) the lab's own python -m lab.pairs on the same data, for comparison with c);
#   f) whether newly produced records free a consumer stuck where the returned batches end.
# Record identity: the value's order_id ("o-<seq>", unique per event; the console consumer prints the value through
# Kafka's BytesDeserializer, raw Fetch decodes it), so records are named the same way with order_id or customer_id keys.
# Environment: KT_PACE (events/s, default 2000; 0 = unthrottled), KT_BATCH (records, default 20000), KT_DEEP (first
# offset of the deep window, default 4000), KT_KEY_FIELD (record key, default order_id; customer_id repeats keys),
# KT_BATCH_NUM_MESSAGES (librdkafka batch.num.messages for the producer, default unset = librdkafka's default;
# 1 = one record per producer batch), KT_FIRST_LOCAL_RETENTION_MS (default unset = the default topic made by
# `make up`; if set, the topic is recreated with SEGMENT_BYTES=1048576 and this local.retention.ms, the lab's replay
# runs once on the local log, then local.retention.ms is set to 30000, as in stage2-replay-source.sh),
# KT_KEEP (a directory: the raw outputs are copied there at the end; they are not part of the transcript).
# Default topic (1 MiB segments, local.retention.ms=30000). Only partition 0's first segment is dumped: a warning is
# printed if it does not hold the whole partition. Ends with make clean.
# Transcripts: stage2-replay-kafka-tools-paced.txt (defaults), -unthrottled.txt (KT_PACE=0),
# -one-record-batches.txt (KT_BATCH=200 KT_PACE=20 KT_DEEP=40: records 50 ms apart, linger.ms=20, so after the first
# one every producer batch holds one record), -one-record-batches-20k.txt (KT_BATCH_NUM_MESSAGES=1: the paced batch
# with one record per producer batch), -customer-keys-unthrottled.txt (KT_KEY_FIELD=customer_id KT_PACE=0
# KT_FIRST_LOCAL_RETENTION_MS=3600000: the configuration of stage2-replay-source-1m-unthrottled.txt).
# Usage: scripts/verify/stage2-replay-kafka-tools.sh > docs/findings/evidence/stage2-replay-kafka-tools-paced.txt 2>&1
source "$(dirname "$0")/lib.sh"

PACE="${KT_PACE:-2000}"
BATCH="${KT_BATCH:-20000}"
DEEP="${KT_DEEP:-4000}"
KEY_FIELD="${KT_KEY_FIELD:-order_id}"
BATCH_NUM="${KT_BATCH_NUM_MESSAGES:-}"
FIRST_LRMS="${KT_FIRST_LOCAL_RETENTION_MS:-}"
TMP="$(mktemp -d)"
trap '[[ -n "${KT_KEEP:-}" ]] && mkdir -p "${KT_KEEP}" && cp -R "${TMP}/." "${KT_KEEP}/"; rm -rf "${TMP}"' EXIT
V=scripts/verify
CC="docker compose exec -T kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:19092 --topic events --property print.partition=true --property print.offset=true --property print.key=true --property print.value=true --property key.deserializer=org.apache.kafka.common.serialization.BytesDeserializer --property value.deserializer=org.apache.kafka.common.serialization.BytesDeserializer"

port_of() {  # host port of broker n (the same variables compose.yml reads)
  case $1 in 1) echo "${KAFKA_1_PORT:-9092}" ;; 2) echo "${KAFKA_2_PORT:-9192}" ;; 3) echo "${KAFKA_3_PORT:-9292}" ;; esac
}

# console <label> <offset> <max messages> <timeout ms>: Kafka's console consumer on partition 0, output kept in $TMP.
console() {
  echo "\$ ${CC#docker compose exec -T kafka-1 } --partition 0 --offset $2 --max-messages $3 --timeout-ms $4 > \$TMP/cc-$1.txt"
  local started=${SECONDS}
  ${CC} --partition 0 --offset "$2" --max-messages "$3" --timeout-ms "$4" >"${TMP}/cc-$1.txt" 2>"${TMP}/cc-$1.err" || true
  grep -E 'Processed a total|ERROR|Exception' "${TMP}/cc-$1.err" | cut -c1-200 || true
  echo "(returned after $((SECONDS - started)) s; $(grep -c '^Partition:' "${TMP}/cc-$1.txt") records written)"
  echo
}

compare_cc() {  # compare_cc <label> <reference jsonl> <reference name>
  echo "\$ python3 -I ${V}/cc_compare.py \$TMP/cc-$1.txt \$TMP/$3 --id order_id   # Kafka console consumer vs $3"
  python3 -I ${V}/cc_compare.py "${TMP}/cc-$1.txt" "$2" --id order_id 2>&1
  echo
}

main() {
set +e  # a failed step is printed, not fatal (wait_for still stops the run)
if [[ -n "${FIRST_LRMS}" ]]; then  # if/else: ${X:+a}${X:-b} would print both a and the value of X
  TOPIC_SETTINGS="recreated with segment.bytes=1048576 local.retention.ms=${FIRST_LRMS}, later 30000"
else
  TOPIC_SETTINGS="default (segment.bytes=1048576, local.retention.ms=30000)"
fi
note "settings: batch ${BATCH} at ${PACE}/s (0 = unthrottled), ${KEY_FIELD} keys (Avro), producer batch.num.messages ${BATCH_NUM:-librdkafka default}; topic: ${TOPIC_SETTINGS}"
run make clean
run make up STAGE=iceberg
if [[ -n "${FIRST_LRMS}" ]]; then
  echo "\$ SEGMENT_BYTES=1048576 LOCAL_RETENTION_MS=${FIRST_LRMS} make topic"
  SEGMENT_BYTES=1048576 LOCAL_RETENTION_MS="${FIRST_LRMS}" make topic 2>&1 | grep -E 'Created topic|Configs:'
  echo
fi
run make topic-describe
LEADER0="$(leader_of 0)"
note "leaders: partition 0 -> broker ${LEADER0}, partition 1 -> broker $(leader_of 1), partition 2 -> broker $(leader_of 2)"

echo "\$ LAB_KEY_FIELD=${KEY_FIELD} LAB_BATCH_NUM_MESSAGES=${BATCH_NUM} make -s batch N=${BATCH} PACE=${PACE}"
LAB_KEY_FIELD="${KEY_FIELD}" LAB_BATCH_NUM_MESSAGES="${BATCH_NUM}" make -s batch N="${BATCH}" PACE="${PACE}" 2>/dev/null
echo

note "a) ground truth while the data is still on the leader's disk: kafka-dump-log.sh on partition 0's segment"
echo "\$ ${OFFSETS#docker compose exec -T kafka-1 } --time -1"; ${OFFSETS} --time -1 2>&1; echo
END0="$(${OFFSETS} --partitions 0 --time -1 2>/dev/null | awk -F: '{print $3}' | tr -d '\r')"
SEG=/var/lib/kafka/data/events-0/00000000000000000000.log
echo "\$ docker compose exec -T kafka-${LEADER0} /opt/kafka/bin/kafka-dump-log.sh --files ${SEG} --print-data-log > \$TMP/dump-p0.txt"
docker compose exec -T "kafka-${LEADER0}" /opt/kafka/bin/kafka-dump-log.sh --files "${SEG}" --print-data-log >"${TMP}/dump-p0.txt" 2>&1 || true
echo "first batch lines of the dump:"
grep -a '^baseOffset' "${TMP}/dump-p0.txt" | head -8 | sed -E 's/ (isTransactional|deleteHorizonMs|CreateTime|crc): [^ ]+//g'
echo "batch lines: $(grep -ac '^baseOffset' "${TMP}/dump-p0.txt"), record lines: $(grep -ac '^| offset' "${TMP}/dump-p0.txt"), isvalid false: $(grep -ac 'isvalid: false' "${TMP}/dump-p0.txt")"
python3 -I ${V}/dump_to_jsonl.py 0 <"${TMP}/dump-p0.txt" >"${TMP}/dump-p0.jsonl"
DUMPED="$(grep -ac '^| offset' "${TMP}/dump-p0.txt")"
if [[ -z "${END0}" ]]; then
  echo "WARNING: the end offset of partition 0 could not be read, so the dump (${DUMPED} records) cannot be checked for"
  echo "         completeness; the comparisons against the original segment below may cover only part of the partition"
elif (( DUMPED < END0 )); then
  echo "WARNING: the dump holds ${DUMPED} records but partition 0 ends at ${END0}: later segments are not dumped, so"
  echo "         the comparisons against the original segment below cover offsets 0..$((DUMPED - 1)) only"
fi
echo
echo "\$ docker compose exec -T kafka-${LEADER0} /opt/kafka/bin/kafka-dump-log.sh --files ${SEG%.log}.index > \$TMP/index-p0.txt   # the segment's offset index"
docker compose exec -T "kafka-${LEADER0}" /opt/kafka/bin/kafka-dump-log.sh --files "${SEG%.log}.index" >"${TMP}/index-p0.txt" 2>&1 || true
grep -a '^offset' "${TMP}/index-p0.txt" | head -4
echo "index entries: $(grep -ac '^offset' "${TMP}/index-p0.txt")"
echo
console local 0 "${END0}" 20000
compare_cc local "${TMP}/dump-p0.jsonl" dump-p0.jsonl
if [[ -n "${FIRST_LRMS}" ]]; then
  note "the lab's replay on the local log (the table may not exist yet: lab.pairs then checks the replay only)"
  pairs_check local
  note "switch to local.retention.ms=30000 so the segment is rolled, uploaded and deleted locally"
  run docker compose exec -T kafka-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-1:19092 --alter --entity-type topics --entity-name events --add-config local.retention.ms=30000
fi

note "wait until the table holds every record and partition 0's first segment is gone from every broker"
wait_rows_at_least "$(end_total)" 240 || true
wait_for 240 "the first segment (base offset 0) is gone from every broker" \
  bash -c '! make -s ls-local 2>/dev/null | grep -q "00000000000000000000.log"'
run make ls-local
SQL_EXPORT='SELECT kafka.partition AS p, kafka."offset" AS o, key, value.order_id AS order_id, kafka.batch_base_offset AS base, kafka.batch_byte_offset AS byte FROM {table} ORDER BY 1, 2'
echo "\$ SQL='${SQL_EXPORT}' make -s query > \$TMP/table.jsonl"
SQL="${SQL_EXPORT}" make -s query >"${TMP}/table.jsonl" 2>/dev/null || echo "(query failed)"
echo "table rows exported: $(grep -c '^{' "${TMP}/table.jsonl")"
echo
query 'SELECT kafka.partition AS p, count(*) AS n, count(DISTINCT kafka."offset") AS distinct_offsets, min(kafka."offset") AS lo, max(kafka."offset") AS hi FROM {table} GROUP BY 1 ORDER BY 1'
note "the table against the original segment (partition 0): key, order_id, batch_base_offset vs dump baseOffset, batch_byte_offset vs dump position"
echo "\$ python3 -I ${V}/rows_compare.py \$TMP/dump-p0.jsonl \$TMP/table.jsonl 0 key order_id base byte"
python3 -I ${V}/rows_compare.py "${TMP}/dump-p0.jsonl" "${TMP}/table.jsonl" 0 key order_id base byte 2>&1
echo
echo "\$ python3 -I ${V}/index_vs_table.py \$TMP/index-p0.txt \$TMP/table.jsonl 0   # batch_byte_offset against the offset index"
python3 -I ${V}/index_vs_table.py "${TMP}/index-p0.txt" "${TMP}/table.jsonl" 0 2>&1
echo
query 'SELECT kafka.batch_base_offset AS base, kafka.batch_byte_offset AS byte, count(*) AS records, min(kafka."offset") AS first_offset, max(kafka."offset") AS last_offset FROM {table} WHERE kafka.partition = 0 GROUP BY ALL ORDER BY first_offset LIMIT 12'

STARTED_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
note "b) Kafka's console consumer, history now served from tiered storage, against the table (partition 0)"
SQL_P0='SELECT kafka."offset" AS o, key, value.order_id AS order_id FROM {table} WHERE kafka.partition = 0 ORDER BY 1'
SQL="${SQL_P0}" make -s query >"${TMP}/table-p0.jsonl" 2>/dev/null || true
console tiered-0 0 2000 30000
compare_cc tiered-0 "${TMP}/table-p0.jsonl" table-p0.jsonl
console tiered-deep "${DEEP}" 2000 30000
compare_cc tiered-deep "${TMP}/table-p0.jsonl" table-p0.jsonl
console tiered-all 0 "${END0}" 20000
compare_cc tiered-all "${TMP}/table-p0.jsonl" table-p0.jsonl
compare_cc tiered-all "${TMP}/dump-p0.jsonl" dump-p0.jsonl

note "c) raw Fetch (v4) to each partition's leader from offset 0, then at (last offset returned + 1), as a consumer would"
for p in 0 1 2; do
  port="$(port_of "$(leader_of "${p}")")"
  echo "\$ python3 -I ${V}/rawfetch.py localhost:${port} events ${p} 0 --walk 3 --records > \$TMP/raw-p${p}.jsonl   # leader: broker $(leader_of "${p}")"
  python3 -I ${V}/rawfetch.py "localhost:${port}" events "${p}" 0 --walk 3 --records >"${TMP}/raw-p${p}.jsonl" 2>"${TMP}/raw-p${p}.err" || echo "(exit code $?)"
  grep -E '^fetch offset' "${TMP}/raw-p${p}.err"
  echo "first batches returned:"
  grep -E '^  batch' "${TMP}/raw-p${p}.err" | head -4
  echo "\$ python3 -I ${V}/fetch_vs_table.py \$TMP/raw-p${p}.jsonl \$TMP/table.jsonl ${p} --id order_id"
  python3 -I ${V}/fetch_vs_table.py "${TMP}/raw-p${p}.jsonl" "${TMP}/table.jsonl" "${p}" --id order_id 2>&1 >/dev/null
  echo
done
echo "\$ python3 -I ${V}/fetch_vs_table.py \$TMP/raw-p0.jsonl \$TMP/table.jsonl 0 \$TMP/dump-p0.jsonl --id order_id   # the same batches, also against the producer's batches"
python3 -I ${V}/fetch_vs_table.py "${TMP}/raw-p0.jsonl" "${TMP}/table.jsonl" 0 "${TMP}/dump-p0.jsonl" --id order_id 2>&1 >/dev/null
echo "\$ python3 -I ${V}/fetch_vs_table.py \$TMP/raw-p0.jsonl \$TMP/dump-p0.jsonl 0 --id order_id   # reference = the original segment ('byte' = producer batch position)"
python3 -I ${V}/fetch_vs_table.py "${TMP}/raw-p0.jsonl" "${TMP}/dump-p0.jsonl" 0 --id order_id 2>&1 >/dev/null
echo
LAST0="$(python3 -c 'import json,sys; b=[x for l in open(sys.argv[1]) for x in json.loads(l)["batches"]]; print(b[-1]["last_offset"] if b else -1)' "${TMP}/raw-p0.jsonl")"
note "raw Fetch at the next offset after the last one partition 0 returned ($((LAST0 + 1))), three times 20 s apart, and at the end offset ${END0}"
for i in 1 2 3; do
  echo "\$ python3 -I ${V}/rawfetch.py localhost:$(port_of "${LEADER0}") events 0 $((LAST0 + 1))   # $(utc_hms) UTC"
  python3 -I ${V}/rawfetch.py "localhost:$(port_of "${LEADER0}")" events 0 "$((LAST0 + 1))" 2>&1 >/dev/null
  (( i < 3 )) && sleep 20
done
echo "\$ python3 -I ${V}/rawfetch.py localhost:$(port_of "${LEADER0}") events 0 ${END0}"
python3 -I ${V}/rawfetch.py "localhost:$(port_of "${LEADER0}")" events 0 "${END0}" 2>&1 >/dev/null
echo
note "d) partition 0 leader (broker ${LEADER0}) log since ${STARTED_UTC}: line kinds, with numbers replaced by N"
docker compose logs --no-log-prefix --since "${STARTED_UTC}" "kafka-${LEADER0}" >"${TMP}/leader.log" 2>&1 || true
echo "lines: $(wc -l <"${TMP}/leader.log" | tr -d ' '), WARN: $(grep -c ' WARN ' "${TMP}/leader.log"), ERROR: $(grep -c ' ERROR ' "${TMP}/leader.log"), lines with 'Exception': $(grep -c 'Exception' "${TMP}/leader.log"), 'Time taken': $(grep -c 'Time taken' "${TMP}/leader.log"), io.aiven loggers: $(grep -c 'io.aiven' "${TMP}/leader.log")"
sed -E 's/^\[[^]]*\] //; s/[0-9]+/N/g' "${TMP}/leader.log" | cut -c1-170 | sort | uniq -c | sort -rn | head -8
echo "remote reads (RemoteLogReader 'Reading records') per minute:"
grep 'Reading records from remote storage' "${TMP}/leader.log" | awk '{print substr($2, 1, 5)}' | uniq -c
echo

note "e) the lab's own tool on the same data (librdkafka), for comparison with c)"
pairs_check tiered

note "f) does new data free a consumer stuck at $((LAST0 + 1))? produce 300 more records, then fetch there again"
echo "\$ make -s batch N=300 START=${BATCH}"
make -s batch N=300 START="${BATCH}" 2>/dev/null
echo
echo "\$ ${OFFSETS#docker compose exec -T kafka-1 } --partitions 0 --time -1"; ${OFFSETS} --partitions 0 --time -1 2>&1
echo "\$ python3 -I ${V}/rawfetch.py localhost:$(port_of "${LEADER0}") events 0 $((LAST0 + 1)),${END0}   # $(utc_hms) UTC"
python3 -I ${V}/rawfetch.py "localhost:$(port_of "${LEADER0}")" events 0 "$((LAST0 + 1)),${END0}" 2>&1 >/dev/null
echo
run make clean
}

# Keep local paths out of the transcript: temporary files show as $TMP, the checkout as '.'.
main 2>&1 | sed -e "s#${TMP}#\$TMP#g" -e "s#${PWD}#.#g"
