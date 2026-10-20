#!/usr/bin/env bash
# (vii) Does the catalog survive? Needs the running stack left by tests/smoke/04-iceberg.sh. DESTRUCTIVE: it
# removes the containers and ends with `make clean`, so run it last.
# 1. `docker compose restart iceberg-rest`: the table must still be there with the same row count.
# 2. `make down` + `make up STAGE=iceberg`: all containers are removed and recreated (volumes kept), so the
#    catalog container starts empty. Then the table is requested and the warehouse bucket listed.
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

note "step 2: make down (removes the containers, keeps the volumes), then make up STAGE=iceberg"
run make down
run make up STAGE=iceberg
wait_for 120 "catalog answers again" catalog_up

note "after removal: namespaces and the table"
echo "\$ curl -s ${REST}/v1/namespaces"
curl -s "${REST}/v1/namespaces"; echo; echo
echo "\$ curl -s -w '\\nHTTP %{http_code}\\n' ${REST}/v1/namespaces/default/tables/events"
curl -s -w '\nHTTP %{http_code}\n' "${REST}/v1/namespaces/default/tables/events"; echo
query 'SELECT count(*) AS row_count FROM {table}'

note "the Parquet files are still in the warehouse bucket (orphaned: nothing in the catalog points at them; inference from Iceberg's layout, not shown here: the metadata and manifests left in warehouse still reference them)"
run scripts/mc.sh ls --recursive local/warehouse/default/events/data
run scripts/mc.sh du local/warehouse

note "reset: stage 2 is reset with make clean"
run make clean
