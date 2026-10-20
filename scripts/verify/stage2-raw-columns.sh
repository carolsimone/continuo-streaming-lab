#!/usr/bin/env bash
# (vi) kafka.timestamp, kafka.batch_*, key_raw, value_raw and headers: what they hold for well-formed Avro records.
source "$(dirname "$0")/lib.sh"

note "null counts: are key_raw and value_raw empty when key and value deserialize as Avro?"
query 'SELECT count(*) AS n, count(key) AS key_not_null, count(key_raw) AS key_raw_not_null, count(value) AS value_not_null, count(value_raw) AS value_raw_not_null FROM {table}'

note "headers: how many headers per record, and one sample"
query 'SELECT len(headers) AS header_count, count(*) AS n FROM {table} GROUP BY 1 ORDER BY 1'
query 'SELECT kafka.partition, kafka."offset", headers FROM {table} ORDER BY kafka.partition, kafka."offset" LIMIT 3'

note "kafka.timestamp against the event timestamp inside the value"
query 'SELECT min(kafka.timestamp) AS min_ts, max(kafka.timestamp) AS max_ts, count(DISTINCT kafka.timestamp) AS distinct_ts, min(kafka.timestamp - value.event_ts_ms) AS min_lag_ms, max(kafka.timestamp - value.event_ts_ms) AS max_lag_ms FROM {table}'
query 'SELECT epoch_ms(min(kafka.timestamp)) AS first_record_time, epoch_ms(max(kafka.timestamp)) AS last_record_time FROM {table}'

note "kafka.batch_*: one row per distinct combination of the constant batch fields"
query 'SELECT kafka.batch_magic, kafka.batch_timestamp_type, kafka.batch_compression_type, kafka.batch_producer_epoch, count(*) AS n, count(DISTINCT kafka.batch_producer_id) AS producer_ids, count(DISTINCT (kafka.partition, kafka.batch_base_offset)) AS batches FROM {table} GROUP BY ALL ORDER BY ALL'

note "kafka.batch_*: how a record relates to its batch (first groups of partition 0, grouped by base offset and byte offset)"
query 'SELECT kafka.batch_base_offset, kafka.batch_byte_offset, count(*) AS records, min(kafka."offset") AS first_offset, max(kafka."offset") AS last_offset FROM {table} WHERE kafka.partition = 0 GROUP BY ALL ORDER BY first_offset LIMIT 8'

note "kafka.batch_*: does one batch_base_offset ever carry more than one batch_byte_offset?"
query 'SELECT count(*) AS batch_base_offsets_with_several_byte_offsets FROM (SELECT kafka.partition AS p, kafka.batch_base_offset AS b FROM {table} GROUP BY ALL HAVING count(DISTINCT kafka.batch_byte_offset) > 1)'
query 'SELECT count(*) AS records_where_offset_before_batch_base_offset FROM {table} WHERE kafka."offset" < kafka.batch_base_offset'
