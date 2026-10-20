# Stage 2: Iceberg table written by the broker, queried with DuckDB

Every number and quoted line below is copied from a file in `docs/findings/evidence/` (named in the text), which is the output of a script in `scripts/verify/` or of `tests/smoke/04-iceberg.sh`. Configuration claims cite `compose.iceberg.yml`. Each statement carries a label: **verified** (shown by a named evidence file), **inference** (my reading of verified facts, not shown directly) or **not tested**. All runs used 3 brokers, topic `events` with 3 partitions and replication factor 3, plugin 1.1.1 in Iceberg mode and `apache/iceberg-rest-fixture:1.10.0` as catalog. They used the default 1 MiB `segment.bytes`, except experiment A in `stage2-failure-modes.txt`, which sets `segment.bytes=262144` on purpose.

Provenance: `stage2-smoke.txt`, `stage2-files.txt`, `stage2-parquet.txt`, `stage2-catalog.txt`, `stage2-raw-columns.txt`, `stage2-batch-fields.txt`, `stage2-key-partition.txt`, `stage2-offsets.txt` and `stage2-persistence.txt` come from one stack lifetime, run in the order given in `scripts/verify/README.md` (the table ids and file names match across them). `stage2-failure-modes.txt` is a separate lifetime. The column layout and the first sample rows were captured earlier, in `stage2-query-describe.txt`.

## What was run

- **verified** (`stage2-smoke.txt`): `tests/smoke/04-iceberg.sh` starts from `make clean`, runs `make up STAGE=iceberg`, waits until the catalog answers `GET /v1/config`, produces 20000 Avro-keyed events with one unthrottled `make batch N=20000` (`produced=20000 failed=0`), then polls the table every 5 s until it holds 20000 distinct `(partition, offset)` pairs. In the first six polls the query returned 0 rows (the table did not exist yet or was empty; the polls only start after the batch was produced, so this covers about 30 s after the batch), the seventh returned `20000`. It then checks that offsets start at 0, rows come from 3 partitions and `kafka-tiered` holds no object, and that `warehouse` holds Parquet. Missing records fail the test; duplicates are counted and printed as a finding. The run ended with `PASS: 20000 of 20000 produced records queryable in Iceberg (20000 rows, 0 duplicates, 3 partitions), kafka-tiered empty, Parquet in warehouse`.
- **verified** (`stage2-key-partition.txt`): a second batch of 20000 events keyed by `customer_id` (`make batch N=20000 START=20000 LAB_KEY_FIELD=customer_id LAB_KEY_SERDE=avro`) on the same stack; the table reached 40000 rows.
- **verified**: the scripts in `scripts/verify/` each print the commands they run and then the real output: `stage2-files.sh`, `stage2-parquet.sh`, `stage2-catalog.sh`, `stage2-raw-columns.sh`, `stage2-batch-fields.sh`, `stage2-key-partition.sh`, `stage2-offsets.sh`, `stage2-persistence.sh`, `stage2-failure-modes.sh`.
- **verified** (`stage2-smoke.txt`, `stage2-files.txt`): the batch finished at 13:55:06 (host local time, from the producer log line) and the Parquet objects carry the time `11:55:41 UTC`, which is 13:55:41 at the host's UTC+2: about 35 s after the last write. **inference**: that is the idle roll of each partition's open segment, as in stage 1; all rows arrived at once because each partition's data was still in one open segment.

## Table schema

Columns come from `stage2-query-describe.txt` (DuckDB `DESCRIBE`) and are confirmed on the Parquet side in `stage2-parquet.txt` (`parquet_schema` of one data file). **Verified**, one line each:

- `kafka` STRUCT, 13 fields, all REQUIRED in Parquet:
  - `partition` INTEGER, `offset` BIGINT, `timestamp` BIGINT (epoch ms);
  - `batch_byte_offset` INTEGER, `batch_base_offset` BIGINT, `batch_partition_leader_epoch` INTEGER, `batch_magic` INTEGER, `batch_timestamp_type` INTEGER, `batch_compression_type` INTEGER, `batch_max_timestamp` BIGINT, `batch_producer_id` BIGINT, `batch_producer_epoch` INTEGER, `batch_base_sequence` INTEGER.
- `headers` `STRUCT("key" VARCHAR, "value" BLOB)[]`.
- `key` VARCHAR (the Avro key decoded to a string).
- `key_raw` BLOB.
- `value` STRUCT(`order_id` VARCHAR, `customer_id` VARCHAR, `event_type` VARCHAR, `amount_cents` BIGINT, `currency` VARCHAR, `event_ts_ms` BIGINT): the Avro value from the schema registry.
- `value_raw` BLOB.

The Parquet schema has 6 top-level fields under the root `table` (`kafka`, `headers`, `key`, `key_raw`, `value`, `value_raw`). The catalog answer has `last-column-id` 28 and `format-version` 2 (`stage2-catalog.txt`).

### The metadata columns, as observed (`stage2-raw-columns.txt`, `stage2-batch-fields.txt`)

- **verified** `key_raw` and `value_raw` are null for well-formed Avro: of 20000 rows, `key_not_null` 20000, `key_raw_not_null` 0, `value_not_null` 20000, `value_raw_not_null` 0. **inference**: they are filled only when deserialization fails (the whitepaper's description); this run did not produce such a record, so the non-null case is **not tested**.
- **verified** `headers` is an empty list on all 20000 rows (`header_count` 0). **inference**: that is because `lab/produce.py` sets no headers; a record with headers was **not tested**.
- **verified** `kafka.timestamp` holds the record time in epoch ms: 132 distinct values in 20000 rows, spanning 131 ms (`11:55:06.631` to `11:55:06.762`). `kafka.timestamp - value.event_ts_ms` is between 0 and 728 ms. `batch_timestamp_type` 0 means CreateTime (a producer-side timestamp), so the lag is not broker time. **inference**: the largest lag belongs to the first record, which waited on schema registration. The arithmetic: if the 728 ms lag is the record with the smallest timestamp (`11:55:06.631`, i.e. `13:55:06.631` host time), its `event_ts_ms` is `13:55:05.903`, which lies between the registry preflight `GET /subjects` (`13:55:05,892`) and the first key-schema lookup (`13:55:05,907`); the key and value schemas were registered at `13:55:06,620` and `13:55:06,630`, just before that first timestamp (`stage2-smoke.txt`, producer log lines). That the max-lag record is the first record is itself an assumption, not shown by a query.
- **verified** `batch_magic` 2, `batch_timestamp_type` 0, `batch_compression_type` 0, `batch_producer_epoch` 0, one `batch_producer_id` for all rows, and 20 distinct `(partition, batch_base_offset)` groups.
- **verified, unexpected** (`stage2-batch-fields.txt`): 17 of the 20 `(partition, batch_base_offset)` groups contain more than one `batch_byte_offset`; in partition 0 the differing record is the last one of each batch. In partition 0 the group `batch_base_offset=744` has 1009 records (offsets 744-1752); 1008 of them (744-1751) carry `batch_byte_offset` 0 and the last one (offset 1752) carries 56885. The next group, base 1753, has 994 records with byte offset 56885 and one (offset 2747) with 134165, and so on. Over all partitions, 17 of the 20 `(partition, batch_base_offset)` groups contain more than one `batch_byte_offset`; the first group of partition 0 (base 0, 744 records) has a single one (0).
- **inference, arithmetic shown** (last query of `stage2-batch-fields.txt`): `batch_base_offset` is right and `batch_byte_offset` lags one batch except on each batch's last record. If the largest byte offset of a group is where that batch starts, the batch sizes come out as 56885 B / 744 records, 77280 B / 1009, 76224 B / 995, 81751 B / 1068, 83480 B / 1091 and 80811 B / 1055, that is 76.46, 76.59, 76.61, 76.55, 76.52 and 76.60 bytes per record: consistent. The reading "the first record of a batch carries the previous base offset" does not fit these numbers. Which field the plugin gets wrong was **not tested** (no comparison with the segment file itself): treat neither field as reliable for grouping records by batch until that is checked; the arithmetic points at `batch_byte_offset`. **Checked later (Task 13b, `03-freshness-and-duplicates.md`, against the original segment dumped with `kafka-dump-log.sh`)**: `batch_base_offset` is right on every row; `batch_byte_offset` equals the segment's offset-index lookup for the record's offset. That lookup gives the previous batch's position, except on a batch's last record, only when every batch is indexed (large batches, as in this run); with small batches one value spans several batches.

## Files in MinIO

- **verified** (`stage2-files.txt`): `warehouse` holds 26 objects, 336 KiB: 3 Parquet data files (78, 77 and 76 KiB, under `default/events/data/`), 11 `*.metadata.json`, 3 manifests (`*-m0.avro`), 6 manifest lists (`snap-*.avro`) and 3 `*.puffin` files (under `default/events/metadata/`).
- **verified** (`stage2-files.txt`, `stage2-smoke.txt`): `kafka-tiered` holds `0 objects`. In Iceberg mode nothing is written to the stage 1 bucket, although its S3 settings are still configured in `compose.yml`. The smoke test asserts this.
- **verified** (`stage2-parquet.txt`): the 3 data files hold 6569, 6646 and 6785 rows (`parquet_file_metadata`, one row group each, written by `parquet-mr version 1.16.0`); `iceberg_metadata` of the current metadata file lists the same 3 files with the same `record_count`; reading `data/*.parquet` as plain Parquet gives 20000 rows. **inference**: one file per uploaded segment, which here is one per partition: the three counts equal the per-partition counts in `stage2-smoke.txt` (6569, 6785, 6646).
- **verified** (`stage2-files.txt`): `mc stat` of one data file: `Size : 76 KiB`, `Type : file`, `Content-Type: application/octet-stream`.
- **verified** (`stage2-files.txt`, `stage2-catalog.txt`): the number of `*.metadata.json` files (11) is higher than the 7 versions the numbering implies (`00000` to `00006`): there are three files each for versions `00000` and `00001`. The catalog shows three empty snapshots whose manifest lists are named `snap-<id>-1-...`, `snap-<id>-2-...` and `snap-<id>-3-...` (the number after the snapshot id looks like an attempt counter). **inference**: the three brokers each created the table and raced to commit to it. Whether the extra files are harmless was **not tested**.

## What the catalog returned

From `stage2-catalog.txt`:

- **verified** `GET /v1/namespaces` returns `{"namespaces":[["default"]],"next-page-token":null}`.
- **verified** `GET /v1/namespaces/default/tables/events` returns `metadata-location` `s3://warehouse/default/events/metadata/00006-8ce92e77-4914-4ad7-9212-7b1d62c135d0.metadata.json`, `location` `s3://warehouse/default/events`, `format-version` 2, `last-sequence-number` 6, followed by the schema (the first 60 lines of the pretty-printed answer contain `metadata-location`, the top-level `metadata` fields up to `last-updated-ms`, `last-column-id` and `current-schema-id`, and the beginning of the schema, i.e. the first `kafka` struct field definitions; the snapshots come after line 60 and are summarised by the next bullet).
- **verified** the catalog holds 6 snapshots, all `append`: three empty ones (`total-records=0`, no `added-records`), then `added-records=6569 / total-records=6569`, `added-records=6646 / total-records=13215` and `added-records=6785 / total-records=20000`, each with `added-data-files=1`, ending at `total-data-files=3`. The current snapshot is the last one.
- **verified** the catalog gave a path, not data: the answer points to a metadata file in `warehouse`; `make query` hands that path to DuckDB's `iceberg_scan`, and DuckDB reads the manifests and Parquet files straight from MinIO (`lab/query.py`; the catalog client only reads `metadata-location`).

## Catalog persistence

This is the answer to spec assumption 4. From `stage2-persistence.txt`, on a stack with 40000 rows:

- **verified** `docker compose restart iceberg-rest`: afterwards the same query returned `row_count` 40000 and the catalog returned the same `metadata-location` (`00009-285669a5-...`). The table survives a restart.
- **verified** `make down` followed by `make up STAGE=iceberg` (all containers removed and recreated, volumes kept): `GET /v1/namespaces` returns `{"namespaces":[],"next-page-token":null}` and `GET /v1/namespaces/default/tables/events` returns HTTP 404 (`NoSuchTableException`, `"Table does not exist: default.events"`). `make query` prints `table default.events does not exist yet: ...`; the tool exits with status 3, which make reports as `make: *** [query] Error 3`, and the transcript then shows `(exit code 2; make reports the tool's exit status 3 as 2)`. That explanatory text was printed by that version of `scripts/verify/lib.sh` after every non-zero exit, whatever the code; it is accurate only here, and the script now prints a plain `(exit code N)`. The transcript was not regenerated, so it is stale in that note and in the heading of the Parquet listing below (the script now says "nothing in the catalog points at them"; the transcript still says "nothing points at them").
- **verified** the 6 Parquet data files (3 per batch) and 41 objects (602 KiB) are still in `warehouse` after the catalog is gone. Nothing in the catalog points at them. **inference** from Iceberg's layout, not shown by any transcript: the metadata and manifest files left in `warehouse` still reference them.
- **inference**: the catalog state is a SQLite file in the container's writable layer (`compose.iceberg.yml` comments; the file location was seen in an earlier task, not in these transcripts). That explains both results. A mounted volume for it was **not tested**.
- **not tested**: whether a later offload recreates the table, and whether the orphaned files would be part of it. **inference**: they would not be, because nothing registers them.
- Consequence, **verified** by the `make down` experiment: reset stage 2 with `make clean` (the script's last step). `make down` loses the catalog.

## Rows vs produced, duplicates and gaps

- **verified** (`stage2-smoke.txt`): 20000 produced, 20000 rows in the table, 20000 distinct `(partition, offset)` pairs; per partition `n` / distinct offsets: partition 0: 6569 / 6569, partition 1: 6785 / 6785, partition 2: 6646 / 6646. Rows are from all 3 partitions and the minimum offset is 0.
- **verified** (`stage2-offsets.txt`, after the second batch, 40000 produced): table rows 12635, 13512 and 13853 for partitions 0, 1 and 2; broker end offsets (`kafka-get-offsets.sh --time -1`) 12635, 13512 and 13853; log start offsets (`--time -2`) 0, 0, 0. Offsets are contiguous from 0 to the last offset in every partition. The comparison line reads `total: missing 0, duplicate rows 0, broker records 40000`.
- **verified**: no gap and no duplicate `(partition, offset)` in the runs recorded here.
- **not tested**: duplicates under broker failure, leader change during offload or catalog loss with re-offload. Duplicates are a known alpha limit of the plugin (stated in the README); this lab did not provoke them.
- **verified** (`stage2-smoke.txt`, `stage2-key-partition.txt`): the table is complete only after the idle roll. Until then the query reported 0 rows or, for the second batch, `20000 of 40000` (the first batch only). The tail of the log is not queryable until its segment is closed and uploaded.

## Key to partition, seen through the table

From `stage2-key-partition.txt` (40000 rows: 20000 keyed `o-...` by `order_id`, 20000 keyed `c-...` by `customer_id`):

- **verified** `SELECT key, count(DISTINCT kafka.partition) AS partitions, count(*) AS n FROM {table} GROUP BY key HAVING count(DISTINCT kafka.partition) > 1` returns no row. Restricted to the `c-` keys the count of keys in more than one partition is 0.
- **verified** 500 distinct `customer_id` keys over 20000 records, and 20000 distinct `order_id` keys over 20000 records.
- **verified** top 5 customer keys by count: `c-0001` 59 (partition 0), `c-0187` 59 (partition 0), `c-0081` 58 (partition 0), `c-0256` 56 (partition 0), `c-0138` 55 (partition 1). Customer keys per partition: 151 keys / 6066 records, 169 / 6727, 180 / 7207.
- **inference**: the guarantee comes from Kafka's key-hash partitioning at the producer, not from Iceberg; the table only shows it. The data files sit flat under `default/events/data/` with no partition sub-directories (`stage2-files.txt`), which suggests the Iceberg table is unpartitioned; the partition spec itself was **not tested** (not inspected).

## Things that fail (`stage2-failure-modes.txt`)

- **verified, `RECORD_LIST_TOO_LARGE`**: with the topic at `segment.bytes=262144` an unthrottled `make batch N=20000` logged `delivery failed: KafkaError{code=RECORD_LIST_TOO_LARGE,val=18,str="Broker: Message batch larger than configured server segment size"}` 20000 times and ended `produced=0 failed=20000`. The count varies by run: an earlier run of the same script (committed in `0ee88c8:docs/findings/evidence/stage2-failure-modes.txt`, lines 108-109; the current transcript is from a later run) ended `produced=13215 failed=6785`. **inference**: a batch built by the producer is larger than the segment, so the broker refuses it.
- **verified, throttling avoids it**: on the same 262144-byte topic, `make batch N=20000 PACE=2000` ended `produced=20000 failed=0` with 0 `RECORD_LIST_TOO_LARGE` lines. The lab keeps the default 1 MiB segment in all other runs.
- **verified, string keys did not offload before the wait ended**: on a clean stack with the default topic (catalog answered HTTP 404 before the batch), `make batch N=2000 LAB_KEY_SERDE=string` (plain UTF-8 keys) was accepted (`produced=2000 failed=0`), and after the wait the broker logs contained `org.apache.kafka.server.log.remote.storage.RemoteStorageException: org.apache.kafka.common.errors.SerializationException: Error deserializing schema ID` on `kafka-1`, `kafka-2` and `kafka-3`, and `Caused by: java.lang.IllegalArgumentException: Unknown magic byte!` on `kafka-3`. The wait stopped at the first broker that logged the error, so the transcript does not show what the other brokers did afterwards. At that point the table did not exist (catalog HTTP 404) and both buckets held 0 objects. **inference**: the plugin deserializes the record key through the schema registry as well as the value; that is why `STAGE=iceberg` makes the producer default to Avro keys.

## Not tested

- An Iceberg reader other than DuckDB (skipped on purpose).
- Schema evolution (the README states it is unsupported by the alpha; this lab did not try to change the schema).
- A catalog with durable storage; partitioned or sorted tables; compaction; table maintenance (the 11 metadata files were not expired or compacted).
- Failure and duplicate scenarios listed under "Rows vs produced".
- (Tested later, Task 13b in `03-freshness-and-duplicates.md`: `batch_base_offset` is right on every row, `batch_byte_offset` follows the segment's offset index.)
