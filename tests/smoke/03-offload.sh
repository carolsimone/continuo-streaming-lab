#!/usr/bin/env bash
# Stage 1: every partition's segments appear in MinIO, local copies disappear, a full replay still works.
source "$(dirname "$0")/../../scripts/lib.sh"

make clean
make up
out="$(make -s batch N=60000 | tail -1)"
produced="$(echo "${out}" | jget produced)"
[[ "${produced}" == "60000" ]] || fail "expected 60000 produced, got ${produced}"

# Experiment 2: each partition has its own prefix and its own segments in the bucket.
for p in 0 1 2; do
  wait_for 120 "partition ${p} segments uploaded to kafka-tiered" \
    bash -c "scripts/mc.sh ls --recursive local/kafka-tiered | grep -qE '/${p}/[0-9]+-.*\.log$'"
done

# Experiment 1 and 3: only the active segment should remain locally on every broker once the
# uploaded segments pass local.retention.ms.
wait_for 120 "old local segments deleted (<= 1 .log left per partition per broker)" \
  bash -c '[ "$(scripts/count-local-segments.sh)" -le 1 ]'

summary="$(make -s replay | tail -1)"
consumed="$(echo "${summary}" | jget consumed)"
[[ "${consumed}" == "${produced}" ]] || fail "replay returned ${consumed}, expected ${produced}"
echo "${summary}" | python3 -c '
import json, sys
s = json.load(sys.stdin)
bad = {p: v["first_offset"] for p, v in s["partitions"].items() if v["first_offset"] != 0}
sys.exit(f"partitions not starting at offset 0: {bad}" if bad else 0)
' || fail "replay did not start at offset 0 in every partition"
log "PASS: ${consumed} records replayed from offset 0 across 3 partitions, old segments served from MinIO"
