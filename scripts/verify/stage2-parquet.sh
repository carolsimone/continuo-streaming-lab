#!/usr/bin/env bash
# (ii) The Parquet files as DuckDB sees them directly: iceberg metadata, per-file row counts and the column schema.
source "$(dirname "$0")/lib.sh"

loc="$(metadata_location)"
listing="$(scripts/mc.sh ls --recursive local/warehouse)"
first="$(grep -o 'default/events/data/[^ ]*\.parquet' <<<"${listing}" | sed -n 1p)"  # sed -n 1p reads all input: no SIGPIPE
[[ -n "${first}" ]] || fail "no Parquet file in the warehouse bucket"

note "data files the Iceberg metadata lists (iceberg_metadata of the current metadata file)"
query "SELECT status, content, file_path, file_format, record_count FROM iceberg_metadata('${loc}') ORDER BY file_path"

note "the same files read as plain Parquet, ignoring Iceberg: rows and row groups per file"
query "SELECT file_name, num_rows, num_row_groups, created_by FROM parquet_file_metadata('s3://warehouse/default/events/data/*.parquet') ORDER BY file_name"

note "total rows over all Parquet files, read as plain Parquet"
query "SELECT count(*) AS rows_in_all_parquet_files FROM read_parquet('s3://warehouse/default/events/data/*.parquet')"

note "column schema of one Parquet file (s3://warehouse/${first})"
query "SELECT name, type, num_children, converted_type, repetition_type FROM parquet_schema('s3://warehouse/${first}')"
