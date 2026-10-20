# continuo-streaming-lab

A local lab for Kafka tiered storage with Aiven's plugin, landing stream data in MinIO as raw
segments (stage 1) and as Iceberg tables you can query with DuckDB (stage 2).

Status: stage 1 (plain tiered storage on a 3-broker cluster, with a leader-failover experiment) and
stage 2 (the broker writes an Iceberg table that DuckDB queries) are done and documented below.
Credentials in this repo (`minioadmin`/`minioadmin`) are demo values for a local stack.

License: Apache-2.0.

## Stage 1: plain tiered storage

### Prerequisites

Docker with Compose v2, `uv`, `make`, `curl`, `unzip`, `shasum` (checks the plugin download in `make plugin`) and a host `python3` (the shell scripts in `scripts/` use it to read JSON). No JDK is needed: Kafka runs in containers
and the Python tools run through `uv`.

The first `make up` downloads the Aiven plugin (version pinned, checksum-verified) into `./plugin`.
If a default host port is already taken, copy the variable you need into a gitignored `.env` file
next to `compose.yml`, for example `MINIO_API_PORT=19000` and `MINIO_CONSOLE_PORT=19001` (the
overridable ports are `MINIO_API_PORT`, `MINIO_CONSOLE_PORT`, `KAFKA_1_PORT`, `KAFKA_2_PORT`,
`KAFKA_3_PORT` and `SCHEMA_REGISTRY_PORT`). `make`, the scripts in `scripts/` and `tests/smoke/` read the same `.env`, so `make replay`, `make batch` and the smoke tests follow the overridden ports; a value already set in your shell wins over `.env`. The commands below assume the default ports.

### Quickstart

Run these from the repository root, in order.

```bash
make up
```
Starts 3 Kafka brokers (KRaft, each both broker and controller), MinIO, Karapace and the `events`
topic. It waits until everything is healthy.

```bash
make topic-describe
```
Shows the topic: 3 partitions, replication factor 3, and which broker leads each partition (the
`Leader:` column) and which replicas are in sync (`Isr:`). Leaders are spread over the brokers.

```bash
make produce
```
Starts the Avro producer in the background (50 events/s by default; `make produce RATE=1000` is
faster, `COUNT=60000` makes it finite). Tiering only moves closed segments, and at the default rate a
partition needs on the order of ten minutes to fill a 1 MiB segment. To see an upload sooner, let it run
for a minute or two, then run `make produce-stop` (below) and wait about 40 seconds: in our runs the brokers
closed the idle active segment roughly `local.retention.ms` after the last write, and the leaders
uploaded it (see the findings).

```bash
make ls-local
```
Lists the `.log` segment files each broker still holds on its own disk. While the first segment is
still open there is one file per partition; once segments are uploaded and `local.retention.ms` has
passed, only the new, empty active segment per partition remains (`0` bytes) on every broker.

```bash
make ls-minio
```
Lists the objects in both buckets, each under its own heading, with the total size at the end of
each: `kafka-tiered` (stage 1 segments, per partition; it reads `0 objects` until the first segment is
uploaded) and `warehouse` (stage 2 Iceberg table; empty in stage 1). Each uploaded stage 1 segment
shows up as a `.log` (segment data), an `.indexes` object and an `.rsm-manifest` object.

```bash
make replay
```
Reads the topic from offset 0 with a plain consumer and prints a JSON summary: records consumed in
total and per partition (`first_offset`, `last_offset`, `high_watermark`). Records no longer on local
disk come back from MinIO.

```bash
make broker-stop BROKER=2
make broker-start BROKER=2
```
Stops and restarts broker 2. Run `make topic-describe VIA=1` in between to see leadership move and the
ISR shrink, then re-form once the broker is back. The producer keeps going.

```bash
make produce-stop
```
Stops the producer; it flushes, and the make target then prints the last two lines of the producer's
container log, which end with a JSON summary of how many events it produced.

```bash
make rebuild
```
Wipes the containers and all data, rebuilds the producer image and starts the stack again from scratch.

```bash
docker compose logs kafka-1 kafka-2 kafka-3 | grep ' Copied '
```
Shows which broker uploaded which segment (the lines the plugin's copy task logs per uploaded segment).

```bash
make clean
```
Removes the containers and all their data (this compose project's volumes only).
`make down` stops the containers but keeps the data (in stage 2 it still loses the Iceberg catalog, see below).

MinIO console: <http://localhost:9001>, user `minioadmin`, password `minioadmin` (demo values for
this local stack, not for anything real). Stage 1 writes to the `kafka-tiered` bucket, stage 2 to `warehouse`.

### How it works

The topic `events` has tiered storage enabled (`remote.storage.enable=true`) with
`segment.bytes=1048576` (1 MiB), `local.retention.ms=30000` and `retention.ms=-1`. Each partition's
leader broker copies its rolled (closed) segments to the `kafka-tiered` bucket through the Aiven
plugin, under its own `events-<topicId>/<partition>/` prefix. Once a segment is in the bucket and is
older than `local.retention.ms`, every replica deletes its local copy, so each broker ends up holding
only the active segment per partition. A consumer that reads below the broker's local start offset is
served by the plugin from the bucket, through that partition's leader.

### Findings

- [docs/findings/01-stage1-offload.md](docs/findings/01-stage1-offload.md): what a run of stage 1
  showed, with the evidence it is based on, and the surprises along the way.
- [docs/findings/01b-leader-failover.md](docs/findings/01b-leader-failover.md): what happens to
  offload and replay when the leader of a partition is stopped.

## Stage 2: Iceberg table, queried with DuckDB

In Iceberg mode the Aiven plugin does not copy raw segments. It converts each uploaded segment to
Parquet and commits it to an Iceberg table (`default.events`) in the `warehouse` bucket, through an
Iceberg REST catalog. Nothing is written to `kafka-tiered`.

```bash
make clean && make up STAGE=iceberg
make batch N=20000 STAGE=iceberg
make query
```

`make query` asks the REST catalog only where the table's current metadata file is; DuckDB then reads
the metadata, the manifests and the Parquet files directly from MinIO. With no argument it prints one
JSON line (row count, partitions, offset range). Pass SQL with `make query SQL='...'` and write the
table as `{table}`; on the make command line a literal dollar sign must be written `$$`. `make ls-minio`
lists both buckets (`kafka-tiered`, empty in this mode, and `warehouse`). The first run needs internet once: DuckDB installs its `iceberg` and
`httpfs` extensions.

- Reset with `make clean`, not `make down`. The catalog is a SQLite file inside the `iceberg-rest`
  container (the findings label this an inference: the stored location of that file was seen in an earlier
  task, not in the committed transcripts). Observed: the table survives `docker compose restart
  iceberg-rest`, but after `make down` and `make up STAGE=iceberg` the catalog is empty and the Parquet
  files stay orphaned in `warehouse`.
- Record keys must be Avro: the plugin reads the key through the schema registry as well. `STAGE=iceberg`
  makes the producer send Avro keys; with string keys the brokers logged `Error deserializing schema ID` (all three brokers) / `Unknown magic byte!`
  (seen on kafka-3 only in the transcript) and no table was created in the run recorded in the findings.
- Rows appear only after a segment is uploaded. The last open segment of each partition uploads after
  the idle roll, about 35 to 40 seconds after the last write, so until then `make query` reports no table
  (the tool exits with status 3, which `make` reports as `Error 3`) or fewer rows.
- Use the default 1 MiB segments: a producer batch larger than `segment.bytes` is rejected with
  `RECORD_LIST_TOO_LARGE`.
- Known alpha limits: duplicate rows are possible in principle (the findings count them for this run) and
  schema evolution is not supported.

Findings, with the evidence they are based on: [docs/findings/02-stage2-iceberg.md](docs/findings/02-stage2-iceberg.md). Freshness and leader-restart results (including that in Iceberg mode the broker serves tiered history under wrong offsets, confirmed with Kafka's own tools and raw Fetch requests): [docs/findings/03-freshness-and-duplicates.md](docs/findings/03-freshness-and-duplicates.md).
The scripts that produced the evidence are in `scripts/verify/`; the end-to-end check is
`tests/smoke/04-iceberg.sh`.
