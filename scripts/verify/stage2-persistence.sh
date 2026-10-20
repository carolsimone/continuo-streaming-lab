#!/usr/bin/env bash
# (vii) Does the catalog survive? Needs the stack from tests/smoke/04-iceberg.sh.
# 1. `docker compose restart iceberg-rest`: the table must still be there with the same row count.
# 2. `docker compose rm -sf iceberg-rest` + `up -d`: the container's writable layer (the SQLite catalog) is gone.
# Ends with `make clean`, because after step 2 the stage 2 state is inconsistent (orphaned Parquet files).
source "$(dirname "$0")/lib.sh"

catalog_up() { curl -fsS "${REST}/v1/config"; }

note "before: rows through the catalog, and the table's metadata pointer"
query 'SELECT count(*) AS row_count FROM {table}'
echo "\$ curl -s ${REST}/v1/namespaces/default/tables/events | python3 (print metadata-location)"
metadata_location
echo

note "step 1: restart the catalog container"
run docker compose restart iceberg-rest
wait_for 120 "catalog answers again" catalog_up
note "after restart: same query, same table"
query 'SELECT count(*) AS row_count FROM {table}'
echo "\$ curl -s ${REST}/v1/namespaces/default/tables/events | python3 (print metadata-location)"
metadata_location
echo

note "step 2: remove the catalog container and start a fresh one"
run docker compose rm -sf iceberg-rest
run docker compose up -d iceberg-rest
wait_for 120 "catalog answers again" catalog_up

note "after removal: namespaces and the table"
echo "\$ curl -s ${REST}/v1/namespaces"
curl -s "${REST}/v1/namespaces"; echo; echo
echo "\$ curl -s -w '\\nHTTP %{http_code}\\n' ${REST}/v1/namespaces/default/tables/events"
curl -s -w '\nHTTP %{http_code}\n' "${REST}/v1/namespaces/default/tables/events"; echo
query 'SELECT count(*) AS row_count FROM {table}'

note "the Parquet files are still in the warehouse bucket (orphaned: nothing points at them)"
run scripts/mc.sh ls --recursive local/warehouse/default/events/data
run scripts/mc.sh du local/warehouse

note "reset: stage 2 is reset with make clean (make down would also lose the catalog)"
run make clean
