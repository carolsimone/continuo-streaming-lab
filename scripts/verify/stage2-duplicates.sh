#!/usr/bin/env bash
# Experiment 7: restart the leader of partition 0 while segments are being offloaded, then compare record
# identities, (partition, offset) pairs, three ways: Iceberg table vs topic replay vs broker end offsets.
#
# Environment (all optional):
#   DUP_TRIGGER=copying|delay  copying (default): restart the moment the leader logs a new "Copying ... to remote storage"
#                              for events-0 after it already finished one upload (an upload in flight).
#                              delay: restart DUP_DELAY seconds (default 4) after that first finished upload.
#   DUP_KILL=0|1               1: SIGKILL the leader and start it again instead of a graceful `docker compose restart`.
# Steps: clean stack, topic with segment.bytes=262144 (small segments, so uploads are frequent), a paced batch keyed by
# customer_id, restart the leader of partition 0 mid-flow, a kicker batch, wait for the table, then
#   A) pairs check with the leaders as they are after the restart,
#   B) stop the broker that leads partition 0 now, wait for a new leader, pairs check again (history served by a
#      broker that did not write those Parquet files for the earlier segments).
# The key-to-partition query runs before the restart, after it (A) and after the stop (B).
# Usage: scripts/verify/stage2-duplicates.sh > docs/findings/evidence/stage2-duplicates.txt 2>&1
source "$(dirname "$0")/lib.sh"

TRIGGER="${DUP_TRIGGER:-copying}"
KILL="${DUP_KILL:-0}"
DELAY="${DUP_DELAY:-4}"
BATCH=100000
KICKER=3000
PACE=2000
WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT
KEY_SQL='SELECT value.customer_id AS customer_id, count(DISTINCT kafka.partition) AS partitions FROM {table} GROUP BY 1 HAVING count(DISTINCT kafka.partition) > 1'
COPY_RE='partition=[^ ]*:events-0\] Copy(ing|ied) '

# wait_log_line <service> <egrep pattern> <seconds>: follow new log lines, succeed when one matches.
wait_log_line() {
  local svc=$1 pattern=$2 deadline=$((SECONDS + $3)) hit="${WORK}/hit"
  : >"${hit}"
  { docker compose logs --no-color -f --tail 0 "${svc}" 2>&1 | grep -m1 -E "${pattern}" >"${hit}"; } &
  local pid=$!
  until [[ -s "${hit}" ]] || (( SECONDS >= deadline )); do sleep 0.1; done
  kill "${pid}" 2>/dev/null || true
  pkill -f "docker compose logs --no-color -f --tail 0 ${svc}" 2>/dev/null || true
  [[ -s "${hit}" ]]
}

# wait_copied <broker> <seconds>: until broker <n> logged a finished upload of events-0.
wait_copied() {
  local deadline=$((SECONDS + $2))
  local logs
  until logs="$(docker compose logs --no-color "kafka-$1" 2>&1)"; grep -qE 'partition=[^ ]*:events-0\] Copied ' <<<"${logs}"; do
    (( SECONDS < deadline )) || return 1
    sleep 1
  done
}

# snapshots: one line per snapshot of the catalog's current table metadata.
snapshots() {
  echo "\$ curl -s ${REST}/v1/namespaces/default/tables/events | python3 (one line per snapshot, time in UTC)"
  curl -s "${REST}/v1/namespaces/default/tables/events" | python3 -c '
import datetime, json, sys
meta = json.load(sys.stdin)["metadata"]
snaps = meta.get("snapshots", [])
print("snapshots:", len(snaps), "| current-snapshot-id:", meta.get("current-snapshot-id"))
for s in snaps:
    t = datetime.datetime.fromtimestamp(s["timestamp-ms"] / 1000, datetime.timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    m = s["summary"]
    print(t, s["snapshot-id"], m.get("operation"), "added-records=" + str(m.get("added-records")),
          "added-data-files=" + str(m.get("added-data-files")), "total-records=" + str(m.get("total-records")))
'
  echo
}

# files_state: Parquet files in the bucket as plain Parquet (offset range per file and partition), overlaps, orphans.
files_state() {
  local loc
  loc="$(metadata_location)"
  local files="WITH f AS (SELECT regexp_extract(filename, '[^/]*\$') AS file, filename AS path, kafka.partition AS p, min(kafka.\"offset\") AS lo, max(kafka.\"offset\") AS hi, count(*) AS n FROM read_parquet('s3://warehouse/default/events/data/*.parquet', filename=true) GROUP BY 1, 2, 3)"
  query "${files} SELECT p, count(*) AS files, sum(n) AS rows, min(lo) AS lowest, max(hi) AS highest, sum(CASE WHEN n = hi - lo + 1 THEN 0 ELSE 1 END) AS files_with_internal_gaps FROM f GROUP BY p ORDER BY p"
  query "${files} SELECT count(*) AS overlapping_file_pairs_same_partition FROM f a JOIN f b ON a.p = b.p AND a.path < b.path AND a.lo <= b.hi AND b.lo <= a.hi"
  query "${files} SELECT count(*) AS parquet_files_in_bucket_not_in_metadata FROM f WHERE path NOT IN (SELECT file_path FROM iceberg_metadata('${loc}'))"
  query "${files} SELECT p, lo, hi, n FROM f ORDER BY p, lo"
}

key_check() {
  note "key mapping ($1): keys that appear in more than one partition (expected: none)"
  query "${KEY_SQL}"
  query "SELECT count(DISTINCT value.customer_id) AS distinct_customer_ids, count(*) AS row_count, count(*) FILTER (WHERE key <> value.customer_id) AS rows_where_key_differs_from_customer_id FROM {table}"
}

log_lines() {  # upload lines of events-0 per broker, plus leadership and errors
  echo "\$ docker compose logs --no-color kafka-1 kafka-2 kafka-3 | grep -E 'events-0.*(Copying|Copied|after becoming leader)'   # broker clock is UTC"
  docker compose logs --no-color kafka-1 kafka-2 kafka-3 2>&1 |
    grep -E 'partition=[^ ]*:events-0\] (Copying|Copied|Found the highest copiedRemoteOffset)' |
    sed -E 's/ \((org\.apache|kafka\.log)[^)]*\)$//; s/with segment-id: .*$/with segment-id: .../; s/ for partition: [^ ]+//' | cut -c1-210
  echo
  echo "\$ docker compose logs --no-color kafka-1 kafka-2 kafka-3 | grep -E 'ERROR|WARN' | grep -iE 'RemoteLogManager|RLMCopyTask|RemoteStorage|iceberg|Failed to copy' | cut -c1-200"
  docker compose logs --no-color kafka-1 kafka-2 kafka-3 2>&1 | grep -E ' (ERROR|WARN) ' |
    grep -iE 'RemoteLogManager|RLMCopyTask|RemoteStorage|iceberg|Failed to copy' | cut -c1-200 || true
  echo
}

note "settings: trigger=${TRIGGER} kill=${KILL} delay=${DELAY} batch=${BATCH} pace=${PACE} kicker=${KICKER} segment.bytes=262144 (everything else lab default)"
run make clean
run make up STAGE=iceberg
echo "\$ SEGMENT_BYTES=262144 make topic"
SEGMENT_BYTES=262144 make topic 2>&1 | grep -E 'Created topic|Configs:|Partition:'
echo

old="$(leader_of 0)"
[[ -n "${old}" ]] || fail "could not read the leader of partition 0"
note "leader of partition 0 before the restart: broker ${old}"

echo "\$ make -s batch N=${BATCH} PACE=${PACE} LAB_KEY_FIELD=customer_id   # in the background, started $(utc_hms) UTC"
make -s batch N=${BATCH} PACE=${PACE} LAB_KEY_FIELD=customer_id >"${WORK}/batch1.out" 2>"${WORK}/batch1.err" &
producer_pid=$!

wait_copied "${old}" 120 || fail "broker ${old} did not finish an upload of events-0 in 120 s"
log "broker ${old} has finished at least one upload of events-0"
note "state before the restart: broker ${old} has already uploaded at least one segment of events-0, the batch is still running"
snapshots
files_state
note "key mapping before the restart (partial table: only the uploaded segments)"
query "${KEY_SQL}"

if [[ "${TRIGGER}" == copying ]]; then
  log "waiting for the next 'Copying' line of events-0 on broker ${old}"
  wait_log_line "kafka-${old}" 'partition=[^ ]*:events-0\] Copying ' 90 || log "no new Copying line within 90 s; restarting anyway"
else
  sleep "${DELAY}"
fi
restart_utc="$(utc_hms)"
if (( KILL )); then
  echo "# restart at ${restart_utc} UTC: SIGKILL, then start"
  run docker compose kill -s KILL "kafka-${old}"
  run docker compose start "kafka-${old}"
else
  echo "# restart at ${restart_utc} UTC: graceful"
  run docker compose restart "kafka-${old}"
fi

note "producer result of the first batch (the restart happened while it was running)"
wait "${producer_pid}" || log "the batch exited non-zero"
cat "${WORK}/batch1.out"
echo "delivery failures logged by the producer: $(grep -c 'delivery failed' "${WORK}/batch1.err" || true)"
echo
wait_for 180 "full ISR for every partition" \
  bash -c '! make -s topic-describe | grep "Partition:" | grep -vq "Isr: [0-9],[0-9],[0-9]"'
note "partition leaders after the restart"
run make topic-describe
new="$(leader_of 0)"
echo "# partition 0 leader after the restart: broker ${new} (before: broker ${old})"
echo

note "kicker batch: ${KICKER} more events so the newest segments close"
echo "\$ make -s batch N=${KICKER} START=${BATCH} PACE=${PACE} LAB_KEY_FIELD=customer_id"
make -s batch N=${KICKER} START=${BATCH} PACE=${PACE} LAB_KEY_FIELD=customer_id 2>"${WORK}/batch2.err"
echo "delivery failures logged by the producer: $(grep -c 'delivery failed' "${WORK}/batch2.err" || true)"
echo

total="$(end_total)"
note "waiting for the table to hold the ${total} records the brokers hold (the open segments upload after the idle roll)"
wait_rows_at_least "${total}" 300 || true
sleep 10

note "state after the restart"
snapshots
files_state
note "broker end offsets (--time -1) and log start offsets (--time -2)"
echo "\$ ${OFFSETS} --time -1"; ${OFFSETS} --time -1 2>&1; echo
echo "\$ ${OFFSETS} --time -2"; ${OFFSETS} --time -2 2>&1; echo
note "what each broker still holds on its own disk (history older than local.retention.ms is served from the table)"
run make ls-local
note "upload lines of events-0 and errors"
log_lines

note "A) three-way comparison after the restart (leader of partition 0: broker ${new})"
pairs_check A
echo "\$ make replay   # the existing replay summary, for the record"
(make -s replay 2>&1 || true) | grep -v ' INFO httpx' | grep -v rdkafka | cut -c1-400
echo
key_check "after the restart"

note "B) stop broker ${new}, the current leader of partition 0, then compare again"
run make broker-stop BROKER="${new}"
export VIA=$(( new == 1 ? 2 : 1 ))  # ask a broker that is still up (make topic-describe VIA=n)
wait_for 120 "a new leader for partition 0 (not broker ${new})" \
  bash -c "make -s topic-describe | grep -E 'Partition: 0[[:space:]]' | grep -E 'Leader: [0-9]+' | grep -vq 'Leader: ${new}[[:space:]]'"
echo "# make topic-describe now asks broker ${VIA} (VIA=${VIA})"
run make topic-describe
pairs_check B
key_check "after stopping the leader"
run make broker-start BROKER="${new}"
unset VIA

note "summary"
snapshots
run make clean
