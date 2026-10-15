#!/usr/bin/env bash
# Experiment 4: stop the leader of partition 0 while producing, then check nothing was lost.
source "$(dirname "$0")/lib.sh"

produce_out="$(mktemp)"
trap 'rm -f "${produce_out}"; [[ -n "${producer_pid:-}" ]] && kill "${producer_pid}" 2>/dev/null || true' EXIT

make clean
make up

make -s batch N=60000 PACE=1000 > "${produce_out}" &
producer_pid=$!
sleep 15

leader="$(make -s topic-describe | grep -E 'Partition: 0\s' | sed -E 's/.*Leader: ([0-9]+).*/\1/')" || leader=""
[[ "${leader}" =~ ^[1-3]$ ]] || fail "could not parse leader of partition 0: '${leader}'"
# Ask a broker that stays up, in case the stopped one is kafka-1.
survivor=$(( leader % 3 + 1 ))
log "before stop:"; make -s topic-describe VIA="${survivor}" >&2
log "stopping broker ${leader}, leader of partition 0"
make broker-stop BROKER="${leader}"
sleep 20
log "after stop:";    make -s topic-describe VIA="${survivor}" >&2

producer_status=0
wait "${producer_pid}" || producer_status=$?
produced="$(tail -1 "${produce_out}" | jget produced)" || produced=unknown

log "restarting broker ${leader}"
make broker-start BROKER="${leader}"
wait_for 120 "full ISR for every partition" \
  bash -c '! make -s topic-describe | grep "Partition:" | grep -vq "Isr: [0-9],[0-9],[0-9]"'
log "after restart:"; make -s topic-describe >&2

(( producer_status == 0 )) || fail "producer exited with status ${producer_status} (produced=${produced})"

summary="$(make -s replay | tail -1)"
consumed="$(echo "${summary}" | jget consumed)"
log "produced=${produced} replayed=${consumed}"
[[ "${produced}" == "${consumed}" ]] || fail "replay returned ${consumed} records, producer got ${produced} acknowledged"
log "PASS: replayed count equals acknowledged count (${produced}) after losing the leader of partition 0; counts only, no per-record comparison"
