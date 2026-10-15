#!/usr/bin/env bash
# Print the largest number of .log files any broker still holds for any events-N partition.
source "$(dirname "$0")/lib.sh"
for n in 1 2 3; do
  docker compose exec -T "kafka-${n}" sh -c \
    'for d in /var/lib/kafka/data/events-*; do ls "$d" | grep -c "\.log$" || true; done'
done | sort -n | tail -1
