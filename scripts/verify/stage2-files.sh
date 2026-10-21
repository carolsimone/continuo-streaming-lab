#!/usr/bin/env bash
# (i) What is in the warehouse bucket after offload, one Parquet file in detail, and what is left in kafka-tiered.
source "$(dirname "$0")/lib.sh"

note "all objects in the warehouse bucket"
run scripts/mc.sh ls --recursive local/warehouse
run scripts/mc.sh du local/warehouse

listing="$(scripts/mc.sh ls --recursive local/warehouse)"
first="$(grep -o 'default/events/data/[^ ]*\.parquet' <<<"${listing}" | sed -n 1p)"  # sed -n 1p reads all input: no SIGPIPE
[[ -n "${first}" ]] || fail "no Parquet file in the warehouse bucket"
note "one Parquet file: stat"
run scripts/mc.sh stat "local/warehouse/${first}"

note "kafka-tiered after offload (stage 1 puts its segments here)"
run scripts/mc.sh ls --recursive local/kafka-tiered
run scripts/mc.sh du local/kafka-tiered
