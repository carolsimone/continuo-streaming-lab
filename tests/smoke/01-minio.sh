#!/usr/bin/env bash
source "$(dirname "$0")/../../scripts/lib.sh"

# 1. A reader has no ghcr.io login: pull both images with an empty Docker config.
# An empty config also drops the active Docker context (for example colima), so pin the
# daemon address of the current context first.
ctx_host="$(docker context inspect --format '{{.Endpoints.docker.Host}}')"
empty="$(mktemp -d)"
DOCKER_HOST="${ctx_host}" DOCKER_CONFIG="${empty}" docker pull ghcr.io/carolsimone/continuo-minio:2025.7.23 >/dev/null \
  || fail "continuo MinIO image does not pull anonymously"
DOCKER_HOST="${ctx_host}" DOCKER_CONFIG="${empty}" docker pull ghcr.io/carolsimone/continuo-mc:2025.7.21 >/dev/null \
  || fail "continuo mc image does not pull anonymously"

# 2. Bucket init succeeds twice (idempotent), 3. buckets exist.
make clean
docker compose up -d --wait minio
docker compose run --rm minio-init >/dev/null || fail "first bucket init failed"
docker compose run --rm minio-init >/dev/null || fail "second bucket init failed"
scripts/mc.sh ls local | grep -q kafka-tiered || fail "kafka-tiered bucket missing"
scripts/mc.sh ls local | grep -q warehouse || fail "warehouse bucket missing"
log "PASS: images pull anonymously and bucket init is idempotent"
