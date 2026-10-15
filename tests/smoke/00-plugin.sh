#!/usr/bin/env bash
source "$(dirname "$0")/../../scripts/lib.sh"
rm -rf plugin
make plugin
ls plugin/core/core-1.1.1.jar plugin/s3/storage-s3-1.1.1.jar >/dev/null || fail "plugin jars missing"
# Capture the listing first: with pipefail, `grep -q` closing the pipe early would SIGPIPE unzip.
listing="$(unzip -l plugin/core/core-1.1.1.jar)"
grep -q 'iceberg/AvroSchemaRegistryStructureProvider.class' <<<"${listing}" \
  || fail "Avro structure provider missing from core jar"
log "PASS: plugin fetched, checksum verified, Iceberg provider present"
