#!/usr/bin/env bash
# (iv) Key to partition, seen through the table. Needs the stack from tests/smoke/04-iceberg.sh (20000 rows,
# keyed by order_id). Produces a second batch of 20000 events keyed by customer_id (Avro keys), waits for the
# table to hold all 40000 rows (the open segments upload after the idle roll), then checks that no key spans
# more than one partition.
source "$(dirname "$0")/lib.sh"

note "second batch: 20000 events, key = customer_id, Avro keys, order sequence continues at 20000"
run make batch N=20000 START=20000 LAB_KEY_FIELD=customer_id LAB_KEY_SERDE=avro

wait_for_rows 40000
note "table rows after the second batch"
query 'SELECT count(*) AS row_count FROM {table}'

note "keys that appear in more than one partition (expected: none)"
query 'SELECT key, count(DISTINCT kafka.partition) AS partitions, count(*) AS n FROM {table} GROUP BY key HAVING count(DISTINCT kafka.partition) > 1'

note "the same check restricted to the customer_id keys of the second batch (keys starting with c-)"
query "SELECT count(*) AS keys_in_more_than_one_partition FROM (SELECT key FROM {table} WHERE key LIKE 'c-%' GROUP BY key HAVING count(DISTINCT kafka.partition) > 1)"

note "how many distinct keys, and how many records carry each kind of key"
query "SELECT count(DISTINCT key) AS distinct_customer_keys, count(*) AS records_with_customer_key FROM {table} WHERE key LIKE 'c-%'"
query "SELECT count(DISTINCT key) AS distinct_order_keys, count(*) AS records_with_order_key FROM {table} WHERE key LIKE 'o-%'"

note "top 5 customer keys by record count, with the partitions they live in"
query "SELECT key, count(*) AS n, list_sort(list(DISTINCT kafka.partition)) AS partitions FROM {table} WHERE key LIKE 'c-%' GROUP BY key ORDER BY n DESC, key LIMIT 5"

note "customer keys per partition (how the c- keys spread over the 3 partitions)"
query "SELECT kafka.partition, count(DISTINCT key) AS customer_keys, count(*) AS records FROM {table} WHERE key LIKE 'c-%' GROUP BY 1 ORDER BY 1"
