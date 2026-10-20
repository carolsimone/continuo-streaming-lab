#!/usr/bin/env bash
# Two ways stage 2 fails, reproduced once each on a clean stack (it runs make clean and make up itself):
# A. a producer batch larger than segment.bytes is rejected by the broker (RECORD_LIST_TOO_LARGE);
# B. string keys: the plugin reads the record key as Avro, so offload fails with "Unknown magic byte!"
#    and no table is created.
source "$(dirname "$0")/lib.sh"

note "start from a clean stage 2 stack"
run make clean
run make up STAGE=iceberg
wait_for 120 "Iceberg REST catalog answers" curl -fsS "${REST}/v1/config"

note "A. topic with segment.bytes=262144, then an unthrottled batch of 20000 Avro-keyed events"
run env SEGMENT_BYTES=262144 make topic
echo "\$ make -s batch N=20000   (output captured; errors and the final line shown)"
batch_out="$(make -s batch N=20000 2>&1 || true)"
# here-strings, not `echo | grep -m`: an early-exiting grep would SIGPIPE echo and trip pipefail
grep -m3 'RECORD_LIST_TOO_LARGE' <<<"${batch_out}" || echo "(no RECORD_LIST_TOO_LARGE line)"
grep 'produced=' <<<"${batch_out}" | tail -1
echo "number of RECORD_LIST_TOO_LARGE lines: $(grep -c 'RECORD_LIST_TOO_LARGE' <<<"${batch_out}" || true)"
echo

note "back to the default 1 MiB segments"
run make topic

note "B. string keys: 2000 events with LAB_KEY_SERDE=string"
run make batch N=2000 LAB_KEY_SERDE=string
broker_has_error() {
  local out
  out="$(docker compose logs --no-color kafka-1 kafka-2 kafka-3 2>&1)"
  grep -q 'Unknown magic byte' <<<"${out}"
}
wait_for 240 "a broker logs 'Unknown magic byte' (the open segment uploads after the idle roll)" broker_has_error
echo "\$ docker compose logs --no-color kafka-1 kafka-2 kafka-3 | grep -m6 -E 'Unknown magic byte|Error deserializing|Failed to'"
logs="$(docker compose logs --no-color kafka-1 kafka-2 kafka-3 2>&1)"
grep -m6 -E 'Unknown magic byte|Error deserializing|Failed to' <<<"${logs}" | cut -c1-400
echo
note "no table was created"
echo "\$ curl -s -w '\\nHTTP %{http_code}\\n' ${REST}/v1/namespaces/default/tables/events"
curl -s -w '\nHTTP %{http_code}\n' "${REST}/v1/namespaces/default/tables/events"; echo
run scripts/mc.sh du local/warehouse
run scripts/mc.sh du local/kafka-tiered

note "reset"
run make clean
