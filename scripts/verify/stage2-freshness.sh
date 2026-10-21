#!/usr/bin/env bash
# Experiment 6: seconds from "marker produced" to "marker row queryable" through the Iceberg table.
# Each run starts from `make clean && make up STAGE=iceberg`, recreates the topic with the settings under test and
# runs `python -m lab.freshness`. Cases (label, segment.ms, local.retention.ms, filler):
#   seg5000 / seg15000 / seg60000: segment.bytes=104857600 so only segment.ms can close the segment; local.retention.ms
#       stays at the lab default 30000; one filler event per second keeps appending after the marker (Kafka checks the
#       time-based roll when a record is appended).
#   seg60000-lrms600000: same as seg60000 but local.retention.ms=600000, to see whether the 30000 default changes the result.
#   control: lab default topic (segment.bytes=1048576, segment.ms unset, local.retention.ms=30000), one marker, NO filler:
#       the open segment is only closed by the idle roll.
# FRESHNESS_REPS (default 3) repeats of every case. A full run takes about 20 minutes.
# Usage: scripts/verify/stage2-freshness.sh > docs/findings/evidence/stage2-freshness.txt 2>&1
source "$(dirname "$0")/lib.sh"

REPS="${FRESHNESS_REPS:-3}"
TIMEOUT=300
SUMMARY=()
WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT

# broker_lines: roll and offload lines of the three brokers for the events topic (UTC, broker clock).
broker_lines() {
  echo "\$ docker compose logs --no-color kafka-1 kafka-2 kafka-3 | grep -E 'Rolled new log segment|RLMCopyTask\\)' | grep -E 'partition=events-|Copying|Copied'"
  docker compose logs --no-color kafka-1 kafka-2 kafka-3 2>&1 |
    grep -E 'Rolled new log segment|RLMCopyTask\)' |
    grep -E 'partition=events-|Copying|Copied' |
    sed -E 's/ \(org\.apache[^)]*\)$//; s/ \(kafka\.log[^)]*\)$//; s/with segment-id: .*$/with segment-id: .../' |
    cut -c1-200 >"${WORK}/broker.lines"
  cat "${WORK}/broker.lines"
  echo
}

# timeline <result json>: first roll, first Copying and first Copied of the marker's partition, as seconds after the marker.
timeline() {
  echo "# timeline of the marker's partition, seconds after the marker was acknowledged (derived from the JSON line and the broker lines above)"
  RESULT="$1" python3 - "${WORK}/broker.lines" <<'PY' 2>&1
import json
import os
import re
import sys

r = json.loads(os.environ["RESULT"])
secs = lambda hms: sum(float(x) * k for x, k in zip(hms.replace(",", ".").split(":"), (3600, 60, 1)))  # noqa: E731
t0 = secs(r["marker_produced_utc"][11:-1])
part = f"events-{r['marker_partition']}"
first = {}
for line in open(sys.argv[1]):
    m = re.search(r"\[\d{4}-\d\d-\d\d (\d\d:\d\d:\d\d,\d+)\].*?(partition=(?:\S+:)?(events-\d)[,\]]).*?(Rolled new log segment|Copying|Copied)", line)
    if m and m.group(3) == part and secs(m.group(1)) >= t0:
        first.setdefault(m.group(4), secs(m.group(1)) - t0)
print(f"marker landed in {part} (offset {r['marker_offset']}); " + "; ".join(
    f"{k}: +{first[k]:.1f} s" if k in first else f"{k}: not seen" for k in ("Rolled new log segment", "Copying", "Copied")
) + f"; marker row seen: +{r['seconds_to_queryable']} s")
PY
  echo
}

# run_case <label> <rep> <segment_ms or ""> <local_retention_ms> <filler_interval>
run_case() {
  local label=$1 rep=$2 seg=$3 lrms=$4 filler=$5 out err rc=0
  note "case ${label}, run ${rep} of ${REPS}"
  run make clean
  run make up STAGE=iceberg
  local env_line="LOCAL_RETENTION_MS=${lrms}"
  [[ -z "${seg}" ]] || env_line="SEGMENT_MS=${seg} SEGMENT_BYTES=104857600 ${env_line}"
  echo "\$ ${env_line} make topic"
  # shellcheck disable=SC2086
  env ${env_line} make topic 2>&1 | grep -E 'Created topic|Configs:'
  echo
  run bash -c "docker compose exec -T kafka-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-1:19092 --describe --all --entity-type topics --entity-name events | grep -E '^  (segment\\.ms|segment\\.bytes|local\\.retention\\.ms)='"
  local cmd="uv run python -m lab.freshness --segment-ms ${seg:-0} --filler-interval ${filler} --poll 2 --timeout ${TIMEOUT}"
  echo "\$ ${cmd}   # stdout = the JSON result, stderr lines below (segment-ms 0 = not set)"
  err="$(mktemp)"
  out="$(${cmd} 2>"${err}")" || rc=$?
  grep -E ' (INFO|ERROR) __main__' "${err}" | cut -c1-240
  rm -f "${err}"
  if (( rc == 0 )); then
    echo "${out}"
    SUMMARY+=("${label} run ${rep}: $(jget seconds_to_queryable <<<"${out}") s, fillers sent $(jget filler_sent <<<"${out}")")
  else
    echo "(exit code ${rc}: timed out or failed)"
    SUMMARY+=("${label} run ${rep}: NO RESULT (exit code ${rc})")
  fi
  echo
  broker_lines
  (( rc != 0 )) || timeline "${out}"
}

for rep in $(seq 1 "${REPS}"); do
  run_case seg5000 "${rep}" 5000 30000 1
  run_case seg15000 "${rep}" 15000 30000 1
  run_case seg60000 "${rep}" 60000 30000 1
  run_case control "${rep}" "" 30000 0
done
run_case seg60000-lrms600000 1 60000 600000 1

note "summary (one line per run)"
printf '%s\n' "${SUMMARY[@]}"
echo
run make clean
