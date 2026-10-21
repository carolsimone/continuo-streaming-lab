#!/usr/bin/env bash
# kafka.batch_base_offset against kafka.batch_byte_offset. Needs the running stack left by tests/smoke/04-iceberg.sh.
# Prints observations only: per (partition, batch_base_offset) group, how many records it has and which
# batch_byte_offset values occur in it, then the arithmetic bytes-per-record under one stated reading.
source "$(dirname "$0")/lib.sh"

note "groups by batch_base_offset in partition 0: records, distinct byte offsets, range of byte offsets, offset range"
query 'SELECT kafka.batch_base_offset AS base, count(*) AS records, count(DISTINCT kafka.batch_byte_offset) AS distinct_byte_offsets, min(kafka.batch_byte_offset) AS min_byte, max(kafka.batch_byte_offset) AS max_byte, min(kafka."offset") AS first_offset, max(kafka."offset") AS last_offset FROM {table} WHERE kafka.partition = 0 GROUP BY ALL ORDER BY base LIMIT 8'

note "the same data grouped by (batch_base_offset, batch_byte_offset): the small groups show where the two fields disagree"
query 'SELECT kafka.batch_base_offset AS base, kafka.batch_byte_offset AS byte, count(*) AS records, min(kafka."offset") AS first_offset, max(kafka."offset") AS last_offset FROM {table} WHERE kafka.partition = 0 GROUP BY ALL ORDER BY first_offset LIMIT 10'

note "how many (partition, batch_base_offset) groups hold more than one batch_byte_offset"
query 'SELECT count(*) AS groups, count(*) FILTER (WHERE n_byte > 1) AS groups_with_several_byte_offsets, count(DISTINCT p) AS partitions FROM (SELECT kafka.partition AS p, kafka.batch_base_offset AS b, count(DISTINCT kafka.batch_byte_offset) AS n_byte FROM {table} GROUP BY ALL)'

note "arithmetic under the reading 'the largest batch_byte_offset of a group is where that batch starts': bytes per record"
query 'WITH g AS (SELECT kafka.batch_base_offset AS base, count(*) AS records, min(kafka.batch_byte_offset) AS min_byte, max(kafka.batch_byte_offset) AS max_byte FROM {table} WHERE kafka.partition = 0 GROUP BY base) SELECT base, records, min_byte, max_byte, lead(max_byte) OVER (ORDER BY base) - max_byte AS bytes_to_next_start, round((lead(max_byte) OVER (ORDER BY base) - max_byte) / records, 2) AS bytes_per_record FROM g ORDER BY base LIMIT 8'
