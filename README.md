# continuo-streaming-lab

A local lab for Kafka tiered storage with Aiven's plugin, landing stream data in MinIO as raw
segments (stage 1) and as Iceberg tables you can query with DuckDB (stage 2).

Status: stage 1 (plain tiered storage on a 3-broker cluster, with a leader-failover experiment) is
done and documented below. Stage 2 (Iceberg mode and DuckDB) is not part of this stage and is not
described here; `STAGE=iceberg` in the Makefile points at a compose file that does not exist yet.
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
Lists the objects in the `kafka-tiered` bucket, per partition, with the total size at the end (it
reads `0 objects` until the first segment is uploaded). Each
uploaded segment shows up as a `.log` (segment data), an `.indexes` object and an `.rsm-manifest`
object.

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
`make down` stops the containers but keeps the data.

MinIO console: <http://localhost:9001>, user `minioadmin`, password `minioadmin` (demo values for
this local stack, not for anything real). The bucket is `kafka-tiered`.

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
