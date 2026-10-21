#!/usr/bin/env bash
# (vi) kafka.timestamp, key_raw, value_raw and headers, and the constant kafka.batch_* fields, for well-formed
# Avro records. Needs the running stack left by tests/smoke/04-iceberg.sh. The batch_* position fields are in
# stage2-batch-fields.sh.
source "$(dirname "$0")/lib.sh"

note "null counts: are key_raw and value_raw empty when key and value deserialize as Avro?"
query 'SELECT count(*) AS n, count(key) AS key_not_null, count(key_raw) AS key_raw_not_null, count(value) AS value_not_null, count(value_raw) AS value_raw_not_null FROM {table}'

note "headers: how many headers per record, and one sample"
query 'SELECT len(headers) AS header_count, count(*) AS n FROM {table} GROUP BY 1 ORDER BY 1'
query 'SELECT kafka.partition, kafka."offset", headers FROM {table} ORDER BY kafka.partition, kafka."offset" LIMIT 3'

note "kafka.timestamp against the event timestamp inside the value"
query 'SELECT min(kafka.timestamp) AS min_ts, max(kafka.timestamp) AS max_ts, count(DISTINCT kafka.timestamp) AS distinct_ts, min(kafka.timestamp - value.event_ts_ms) AS min_lag_ms, max(kafka.timestamp - value.event_ts_ms) AS max_lag_ms FROM {table}'
query 'SELECT epoch_ms(min(kafka.timestamp)) AS first_record_time, epoch_ms(max(kafka.timestamp)) AS last_record_time FROM {table}'

note "kafka.batch_*: one row per distinct combination of the constant batch fields (batches = distinct (partition, batch_base_offset))"
query 'SELECT kafka.batch_magic, kafka.batch_timestamp_type, kafka.batch_compression_type, kafka.batch_producer_epoch, count(*) AS n, count(DISTINCT kafka.batch_producer_id) AS producer_ids, count(DISTINCT (kafka.partition, kafka.batch_base_offset)) AS batches FROM {table} GROUP BY ALL ORDER BY ALL'
