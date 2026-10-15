#!/usr/bin/env bash
# Run the MinIO client against the lab MinIO: scripts/mc.sh ls --recursive local/kafka-tiered
source "$(dirname "$0")/lib.sh"
docker compose run --rm --no-deps -T --entrypoint sh minio-init \
  -c 'mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null && mc "$@"' sh "$@"

