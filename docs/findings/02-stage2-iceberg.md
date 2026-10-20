# Stage 2: Iceberg table written by the broker, queried with DuckDB

Every number and quoted line below is copied from a file in `docs/findings/evidence/` (named in the text), which is the output of a script in `scripts/verify/` or of `tests/smoke/04-iceberg.sh`. Configuration claims cite `compose.iceberg.yml`. Each statement carries a label: **verified** (shown by a named evidence file), **inference** (my reading of verified facts, not shown directly) or **not tested**. All runs used 3 brokers, the default 1 MiB `segment.bytes`, topic `events` with 3 partitions and replication factor 3, plugin 1.1.1 in Iceberg mode, and `apache/iceberg-rest-fixture:1.10.0` as catalog.

Two runs of the smoke test back this file. `stage2-smoke.txt`, `stage2-files.txt`, `stage2-parquet.txt`, `stage2-catalog.txt`, `stage2-key-partition.txt`, `stage2-offsets.txt` and `stage2-persistence.txt` come from one stack lifetime (the table ids and file names match across them). `stage2-raw-columns.txt` was captured on a second, identical run, so its file names and timestamps differ. `stage2-failure-modes.txt` is a third, separate lifetime. The column layout and the first sample rows were captured earlier in `stage2-query-describe.txt`.

## What was run

- **verified** (`stage2-smoke.txt`): `tests/smoke/04-iceberg.sh` starts from `make clean`, runs `make up STAGE=iceberg`, waits until the catalog answers `GET /v1/config`, produces 20000 Avro-keyed events with one unthrottled `make batch N=20000` (`produced=20000 failed=0`), then polls the table every 5 s. The table showed `0` rows in six polls and `20000` in the seventh. It then checks offsets start at 0, rows come from 3 partitions, `(partition, offset)` pairs are distinct, `kafka-tiered` holds no object and `warehouse` holds Parquet. The run ended with `PASS: 20000 of 20000 produced records queryable in Iceberg (3 partitions, no duplicate offsets), kafka-tiered empty, Parquet in warehouse`.
- **verified** (`stage2-key-partition.txt`): a second batch of 20000 events keyed by `customer_id` (`--key-field customer_id`, Avro keys, sequence continued at 20000) on the same stack; the table reached 40000 rows.
- **verified**: the scripts in `scripts/verify/` each print the commands they run and then the real output: `stage2-files.sh`, `stage2-parquet.sh`, `stage2-catalog.sh`, `stage2-raw-columns.sh`, `stage2-key-partition.sh`, `stage2-offsets.sh`, `stage2-persistence.sh`, `stage2-failure-modes.sh`.
- **verified** (`stage2-smoke.txt`): rows were not visible while the batch was being produced. The batch was produced at 13:21:55 (local time of the host, from the producer log line in `stage2-smoke.txt`) and the Parquet files carry the object time `11:22:30 UTC` (`stage2-files.txt`), the same instant at UTC+2. That is about 35 s after the last write. **inference**: this is the idle roll of each partition's open segment, as in stage 1; every row arrived at once because each partition's data was still in one open segment.

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

### The metadata columns, as observed (`stage2-raw-columns.txt`)

- **verified** `key_raw` and `value_raw` are null for well-formed Avro: of 20000 rows, `key_not_null` 20000, `key_raw_not_null` 0, `value_not_null` 20000, `value_raw_not_null` 0. **inference**: they are filled only when deserialization fails (the whitepaper's description); this run did not produce such a record, so the non-null case is **not tested**.
- **verified** `headers` is an empty list on all 20000 rows (`header_count` 0). **inference**: that is because `lab/produce.py` sets no headers; a record with headers was **not tested**.
- **verified** `kafka.timestamp` holds the record time in epoch ms: 138 distinct values in 20000 rows, and `kafka.timestamp - value.event_ts_ms` between 0 and 1736 ms (the event time is set when the event is built, the record time when the producer batches it).
- **verified** `batch_magic` 2, `batch_timestamp_type` 0, `batch_compression_type` 0, `batch_producer_epoch` 0, one `batch_producer_id` for all rows, and 0 records with `offset < batch_base_offset`.
- **verified, unexpected**: `batch_*` is not constant per Kafka batch. The query grouping partition 0 by `(batch_base_offset, batch_byte_offset)` shows groups such as `base 584 / byte 0` with 1097 records (offsets 584-1680), then `base 584 / byte 44643` with a single record (offset 1681), then `base 1682 / byte 44643` with 1040 records. So the first record of a batch carries the previous batch's `batch_base_offset` together with the new batch's `batch_byte_offset`: 18 `(partition, batch_base_offset)` values appear with more than one `batch_byte_offset`. **inference**: an off-by-one in how the alpha plugin fills `batch_base_offset`. Do not use `batch_base_offset` to group records into batches without checking it.

## Files in MinIO

- **verified** (`stage2-files.txt`): `warehouse` holds 26 objects, 336 KiB: 3 Parquet data files (78, 77 and 76 KiB, under `default/events/data/`), 11 `*.metadata.json`, 3 manifests (`*-m0.avro`), 6 manifest lists (`snap-*.avro`) and 3 `*.puffin` files (under `default/events/metadata/`).
- **verified** (`stage2-files.txt`, `stage2-smoke.txt`): `kafka-tiered` holds `0 objects`. In Iceberg mode nothing is written to the stage 1 bucket, although its S3 settings are still configured in `compose.yml`. The smoke test asserts this.
- **verified** (`stage2-parquet.txt`): the 3 data files hold 6785, 6646 and 6569 rows (`parquet_file_metadata`, one row group each, written by `parquet-mr version 1.16.0`); `iceberg_metadata` of the current metadata file lists the same 3 files with the same `record_count`; reading `data/*.parquet` as plain Parquet gives 20000 rows. **inference**: one file per uploaded segment, which here is one per partition: the three counts equal the per-partition counts in `stage2-smoke.txt` (6569, 6785, 6646).
- **verified** (`stage2-files.txt`): `mc stat` of one data file: `Size : 78 KiB`, `Type : file`, `Content-Type: application/octet-stream`.
- **verified** (`stage2-files.txt`, `stage2-catalog.txt`): the number of `*.metadata.json` files (11) is higher than the 7 versions the numbering implies (`00000` to `00006`): there are three files each for versions `00000` and `00001`. **inference**: the three brokers each created the table and raced to commit to it. This matches the catalog showing three empty snapshots (below). Whether the extra files are harmless was **not tested**.

## What the catalog returned

From `stage2-catalog.txt`:

- **verified** `GET /v1/namespaces` returns `{"namespaces":[["default"]],"next-page-token":null}`.
- **verified** `GET /v1/namespaces/default/tables/events` returns `metadata-location` `s3://warehouse/default/events/metadata/00006-c3a11d6c-5fe0-4bc5-bfb8-bb4eac19bdac.metadata.json`, `location` `s3://warehouse/default/events`, `format-version` 2, `last-sequence-number` 6, followed by the schema (the first 60 lines of the pretty-printed answer are the `kafka` struct's field definitions).
- **verified** the catalog holds 6 snapshots, all `append`: three empty ones (`total-records=0`, no `added-records`), then `added-records=6785 / total-records=6785`, `added-records=6646 / total-records=13431` and `added-records=6569 / total-records=20000`, each with `added-data-files=1`, ending at `total-data-files=3`. The current snapshot is the last one.
- **verified** the catalog gave a path, not data: the answer points to a metadata file in `warehouse`; `make query` hands that path to DuckDB's `iceberg_scan`, and DuckDB reads the manifests and Parquet files straight from MinIO (`lab/query.py`; the catalog client only reads `metadata-location`).

## Catalog persistence

This is the answer to spec assumption 4. From `stage2-persistence.txt`, on a stack with 40000 rows:

- **verified** `docker compose restart iceberg-rest`: afterwards the same query returned `row_count` 40000 and the catalog returned the same `metadata-location` (`00009-66628a65-...`). The table survives a restart.
- **verified** `docker compose rm -sf iceberg-rest` followed by `docker compose up -d iceberg-rest`: `GET /v1/namespaces` returns `{"namespaces":[],"next-page-token":null}` and `GET /v1/namespaces/default/tables/events` returns HTTP 404 (`NoSuchTableException`, `"Table does not exist: default.events"`). `make query` prints `table default.events does not exist yet: ...` and exits with the tool's code 3 (the transcript shows `(exit code 2)`, which is make's own code for a failed recipe).
- **verified** the 6 Parquet data files (3 per batch) and 41 objects (601 KiB) are still in `warehouse` after the catalog is gone. Nothing points at them.
- **inference**: the catalog state is a SQLite file in the container's writable layer (`compose.iceberg.yml` comments; the file location was seen in an earlier task, not in these transcripts). That explains both results. A mounted volume for it was **not tested**.
- **not tested**: whether a later offload recreates the table and whether it would include the orphaned files (it would not, by construction, unless they were registered).
- Consequence, **verified** by the experiment: reset stage 2 with `make clean` (the script's last step). `make down` removes the container and loses the catalog.

## Rows vs produced, duplicates and gaps

- **verified** (`stage2-smoke.txt`): 20000 produced, 20000 rows in the table; per partition `n` / distinct offsets: partition 0: 6569 / 6569, partition 1: 6785 / 6785, partition 2: 6646 / 6646. Rows are from all 3 partitions and the minimum offset is 0.
- **verified** (`stage2-offsets.txt`, after the second batch, 40000 produced): table rows 12635, 13512 and 13853 for partitions 0, 1 and 2; broker end offsets (`kafka-get-offsets.sh --time -1`) 12635, 13512 and 13853; log start offsets (`--time -2`) 0, 0, 0. Offsets are contiguous from 0 to the last offset in every partition. The comparison line reads `total: missing 0, duplicate rows 0, broker records 40000`.
- **verified**: no gap and no duplicate `(partition, offset)` in the runs recorded here. A duplicate would have failed the smoke test.
- **not tested**: duplicates under broker failure, leader change during offload or catalog loss with re-offload. Duplicates are a known alpha limit of the plugin (stated in the README); this lab did not provoke them.
- **verified** (`stage2-smoke.txt`, `stage2-key-partition.txt`): the table is complete only after the idle roll. Until then the query reported 0 rows or, for the second batch, `20000 of 40000` (the first batch only). The tail of the log is not queryable until its segment is closed and uploaded.

## Key to partition, seen through the table

From `stage2-key-partition.txt` (40000 rows: 20000 keyed `o-...` by `order_id`, 20000 keyed `c-...` by `customer_id`):

- **verified** `SELECT key, count(DISTINCT kafka.partition) AS partitions, count(*) AS n FROM {table} GROUP BY key HAVING count(DISTINCT kafka.partition) > 1` returns no row. Restricted to the `c-` keys the count of keys in more than one partition is 0.
- **verified** 500 distinct `customer_id` keys over 20000 records, and 20000 distinct `order_id` keys over 20000 records.
- **verified** top 5 customer keys by count: `c-0001` 59 (partition 0), `c-0187` 59 (partition 0), `c-0081` 58 (partition 0), `c-0256` 56 (partition 0), `c-0138` 55 (partition 1). Customer keys per partition: 151 keys / 6066 records, 169 / 6727, 180 / 7207.
- **inference**: the guarantee comes from Kafka's key-hash partitioning at the producer, not from Iceberg; the table only shows it. The table is not partitioned by `kafka.partition` or by key (the Iceberg partition spec was not inspected: **not tested**).

## Things that fail (`stage2-failure-modes.txt`)

- **verified, `RECORD_LIST_TOO_LARGE`**: with the topic at `segment.bytes=262144` an unthrottled `make batch N=20000` logged `delivery failed: KafkaError{code=RECORD_LIST_TOO_LARGE,val=18,str="Broker: Message batch larger than configured server segment size"}` 6785 times and ended `produced=13215 failed=6785`. The count varies by run: an earlier run of the same script (its transcript was overwritten) ended `produced=0 failed=20000`. **inference**: a batch built by the producer is larger than the segment, so the broker refuses it; the lab keeps the default 1 MiB segment (all runs above) and `make topic SEGMENT_BYTES=...` must stay above the producer batch size, or the batch must be throttled with `PACE`.
- **verified, Avro keys are required**: after `make batch N=2000 LAB_KEY_SERDE=string` (plain UTF-8 keys) all 2000 records were accepted (`produced=2000 failed=0`), but no segment could be offloaded. The broker logs contain, on `kafka-2` and `kafka-3` in this run: `org.apache.kafka.server.log.remote.storage.RemoteStorageException: org.apache.kafka.common.errors.SerializationException: Error deserializing schema ID`, `Caused by: java.lang.IllegalArgumentException: Unknown magic byte!`. The table does not exist afterwards (catalog HTTP 404) and both buckets hold 0 objects. **inference**: the plugin deserializes the record key through the schema registry as well as the value; that is why `STAGE=iceberg` makes the producer default to Avro keys.

## Not tested

- An Iceberg reader other than DuckDB (skipped on purpose).
- Schema evolution (the README states it is unsupported by the alpha; this lab did not try to change the schema).
- A catalog with durable storage; multi-partition-spec or sorted tables; compaction; table maintenance (the 11 metadata files were not expired or compacted).
- Failure and duplicate scenarios listed under "Rows vs produced".
