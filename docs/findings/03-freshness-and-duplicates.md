# Stage 2: freshness (experiment 6) and duplicates (experiment 7)

Every number below is copied from a transcript in `docs/findings/evidence/` (named in the text), produced by a script in `scripts/verify/`. Each statement carries a label: **verified** (shown by the named transcript), **inference** (my reading of verified facts, not shown directly) or **not tested**. All runs used 3 brokers, topic `events` with 3 partitions and replication factor 3, Kafka 4.0.0 (`apache/kafka:4.0.0`), the Aiven plugin 1.1.1 in Iceberg mode (alpha) and the `apache/iceberg-rest-fixture:1.10.0` catalog, the lab's client confluent-kafka 2.16.0 (librdkafka 2.16.0), each from `make clean && make up STAGE=iceberg`. Times in broker log lines are UTC (the broker clock); the harness converts its own times to UTC too, so the two are comparable to about 0.1 s (a "Copied" line can be timed up to 0.04 s after the row was already seen, see below).

Transcripts and the scripts that wrote them:

| Transcript | Script | What |
|---|---|---|
| `stage2-freshness.txt` | `scripts/verify/stage2-freshness.sh` (uses `lab/freshness.py`) | experiment 6, 13 runs |
| `stage2-duplicates-1.txt`, `-2.txt`, `-3.txt` | `scripts/verify/stage2-duplicates.sh` (uses `lab/pairs.py`) | experiment 7, three runs |
| `stage2-replay-source-256k-paced.txt`, `-1m-unthrottled.txt`, `-1m-paced.txt` | `scripts/verify/stage2-replay-source.sh` (uses `lab/pairs.py`) | control for the replay results of experiment 7 |
| `stage2-replay-kafka-tools-paced.txt`, `-one-record-batches-20k.txt`, `-unthrottled.txt`, `-customer-keys-unthrottled.txt`, `-one-record-batches.txt` | `scripts/verify/stage2-replay-kafka-tools.sh` (Kafka's own `kafka-dump-log.sh` and `kafka-console-consumer.sh`, raw Fetch requests; `lab.pairs` last, for comparison) | independent check of the replay result (Task 13b) |

## Experiment 6: how long until a produced record is queryable

### Method

- `lab/freshness.py` produces one marker event (Avro key and value, order id `marker-<8 hex>`), starts the clock when the broker acknowledged it, then (when filler is on) produces one filler event per second and checks the table: it asks the catalog for the table and counts rows with `value.order_id = '<marker>'` through DuckDB (a new connection per check), then waits 1 s (the filler interval) in the filler cases or 2 s (`--poll 2`) in the control before the next check, so a check starts about every 1 s, or 2 s, plus the query time. It prints one JSON line with `seconds_to_queryable`, the partition and offset the marker landed in, and UTC timestamps.
- Cases (`stage2-freshness.sh`, 3 runs of each; the last case 1 run), each from a clean stack and a recreated topic:
  - `seg5000`, `seg15000`, `seg60000`: `SEGMENT_MS=<ms> SEGMENT_BYTES=104857600 LOCAL_RETENTION_MS=30000 make topic`, then `uv run python -m lab.freshness --segment-ms <ms> --filler-interval 1 --poll 2 --timeout 300`.
  - `seg60000-lrms600000` (1 run): as `seg60000` but `LOCAL_RETENTION_MS=600000`.
  - `control`: the lab's default topic (`LOCAL_RETENTION_MS=30000`, `segment.bytes=1048576`, `segment.ms` not set; the transcript shows the broker default `segment.ms=604800000`), one marker and **no filler** (`--filler-interval 0`).
- What the setting under test is, and what stayed constant: **verified** (`stage2-freshness.txt`, the `kafka-configs --describe` lines of every run): in the three `segment.ms` cases `segment.bytes` was 104857600, so the size limit cannot close the segment in these runs (the marker plus about 60 fillers are a few kB); `local.retention.ms` was 30000 (the lab default) except in `seg60000-lrms600000`; two broker intervals are set by the lab, not left at Kafka's defaults: `remote.log.manager.task.interval.ms=5000` and `log.retention.check.interval.ms=5000` (`compose.yml`, `KAFKA_REMOTE_LOG_MANAGER_TASK_INTERVAL_MS` and `KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS`; Kafka's defaults are 30 s and 5 min; **inference**: those would likely change both the filler and the control results; other values **not tested**); the harness checks the table about every 1 s in the filler cases and about every 2 s in the control, plus the query time; one producer, tiny rows, no other load. The three partitions each got about a third of the fillers (random keys), so a given partition sees a write about every 3 s.

### Headline table

Seconds from marker acknowledged to marker row returned by the query (`stage2-freshness.txt`, `## summary` and the JSON lines). **verified**.

| Setting | Runs | Seconds to queryable | Range |
|---|---|---|---|
| `segment.ms=5000` (filler) | 3 | 13.49, 7.24, 8.55 | 7.24 to 13.49 |
| `segment.ms=15000` (filler) | 3 | 20.86, 22.76, 22.57 | 20.86 to 22.76 |
| `segment.ms=60000` (filler) | 3 | 64.29, 72.27, 60.96 | 60.96 to 72.27 |
| `segment.ms=60000`, `local.retention.ms=600000` (filler) | 1 | 62.82 | |
| default topic, single marker, no filler (control) | 3 | 34.23, 38.20, 34.22 | 34.22 to 38.20 |

No run timed out or failed (the transcript has no `exit code` line). The number of fillers sent is about one per second waited (14 for 13.49 s, 64 for 64.29 s, 72 for 72.27 s).

### What the broker logs show per run

**Verified** (`stage2-freshness.txt`, every run prints the lines it derives this from): in Iceberg mode the upload shows up in the broker logs with the same INFO lines as in stage 1, so the instruction to look for a different line was not needed here: `[RemoteLogManager=<id> partition=<topic id>:events-<p>] Copying 0000...N.log to remote storage.` and, 0.7 to 1.8 s later, `Copied 0000...N.log to remote storage with segment-id: ...` (both from `RLMCopyTask`), and, on all three replicas at the same moment, `[LocalLog partition=events-<p> ...] Rolled new log segment at offset N`. In the experiment 7 transcripts the `<id>` in `RemoteLogManager=<id>` is the broker that `make topic-describe` shows as leader of that partition, and only that broker logs `Copying`; I did not print the leaders in the freshness runs. The `kafka.log.remote` DEBUG lines are not needed.

Per run, for the partition the marker landed in (seconds after the marker was acknowledged; the script prints this as `marker landed in events-<p> ...`):

| Run | Partition | Segment rolled | `Copying` | `Copied` | Row seen |
|---|---|---|---|---|---|
| seg5000 #1 | 1 | 11.3 | 12.1 | 13.4 | 13.49 |
| seg5000 #2 | 0 | 5.1 | 5.1 | 6.9 | 7.24 |
| seg5000 #3 | 2 | 4.4 | 6.2 | 7.9 | 8.55 |
| seg15000 #1 | 2 | 16.7 | 19.4 | 20.9 | 20.86 |
| seg15000 #2 | 0 | 17.5 | 21.7 | 22.4 | 22.76 |
| seg15000 #3 | 2 | 16.5 | 20.9 | 22.0 | 22.57 |
| seg60000 #1 | 1 | 61.2 | 61.8 | 63.4 | 64.29 |
| seg60000 #2 | 2 | 71.2 | 71.2 | 72.1 | 72.27 |
| seg60000 #3 | 0 | 58.9 | 59.3 | 60.9 | 60.96 |
| seg60000, lrms 600000 | 1 | 60.8 | 61.2 | 62.6 | 62.82 |
| control #1 | 2 | 30.3 | 31.8 | 33.2 | 34.23 |
| control #2 | 0 | 31.9 | 36.9 | 38.2 | 38.20 |
| control #3 | 0 | 30.9 | 32.2 | 33.7 | 34.22 |

Computed from that table (all 13 runs): `Copying` comes 0.0 to 5.0 s after the roll, the copy takes 0.7 to 1.8 s, and the row is seen -0.04 to 1.03 s after the `Copied` line (the negative value is clock skew between the broker log and the harness, about 0.1 s at most).

### Reading the numbers

- **verified**: with continuous writes, `segment.ms` decided: the segment of the marker rolled 4.4 to 11.3 s after the marker with `segment.ms=5000`, 16.5 to 17.5 s with 15000 and 58.9 to 71.2 s with 60000. The roll came at or after `segment.ms` except twice (4.4 s against 5000, 58.9 s against 60000). **inference**: the segment's age probably counts from the record's own timestamp, which is taken before the send, while my clock starts after the acknowledgement, so a roll 0.6 s or 1.1 s "early" on my clock is plausible; I did not measure this.
- **verified**: the default 30000 of `local.retention.ms` did not shorten the 60000 case: `seg60000` (30000) rolled at 58.9, 61.2 and 71.2 s, `seg60000-lrms600000` rolled at 60.8 s. This only shows that with fillers every partition keeps getting writes; it does not show what `local.retention.ms` would do for a partition that goes quiet, which is the control.
- **verified**: without any further write (control), the marker's segment rolled 30.3 to 31.9 s after the marker and the row was queryable after 34.2 to 38.2 s. **inference**: the roll is the idle roll about `local.retention.ms` (30000) after the last write, plus the broker's periodic checks (the lab sets `log.retention.check.interval.ms` and `remote.log.manager.task.interval.ms` to 5000 in `compose.yml`, against Kafka's defaults of 5 min and 30 s; I did not vary them or `local.retention.ms`, so the dependence is **not tested**). It agrees with the 35 to 40 s measured in `docs/findings/02-stage2-iceberg.md`.
- **inference**, not a formula: the results are consistent with `delay = time until the segment rolls + 0 to 5 s until the next upload cycle + about 1 s copy + at most about 1 s (filler cases) or about 2 s (control) until my check, plus the query time`. The time until the segment rolls is `segment.ms` plus up to about 11 s of slack in my runs when writes keep arriving, and about `local.retention.ms` when they do not. The slack with `segment.ms` is consistent with Kafka evaluating the roll when a record is appended to that partition (the topic note in `lab/freshness.py`): the fillers reach a given partition about every 3 s on average, and the largest slack (+11.2 s in `seg60000` #2, +6.3 s in `seg5000` #1) is more than that, so I cannot say what else was waiting. **not tested**: any `segment.ms` below 5000, any other `local.retention.ms` in the idle case, any broker interval, higher rates, many partitions rolling at once, larger rows.
- **verified, side effect of the design**: the harness sees the row only after the whole segment is uploaded, so one visible row means all rows of that segment before it are visible too.

### What this means for an ingestion service with a "queryable within N seconds" guarantee

- **verified** (`stage2-freshness.txt`, `## summary`): under steady writes to every partition, the observed delay was about `segment.ms` plus 1 to 12 s (7.2 to 13.5 s at 5000, 20.9 to 22.8 s at 15000, 61.0 to 72.3 s at 60000, 3 runs each), and the largest observed overhead (seconds queryable minus `segment.ms`) was 12.3 s (`seg60000` run 2), in the 10 filler runs (these 9 and `seg60000-lrms600000`). **inference**: a guarantee of N seconds would need `segment.ms` of at most about N minus that overhead in this setup (one producer, these broker intervals); nothing below `segment.ms=5000` was tested, and 3 runs per value do not bound the overhead.
- **verified** (control, 3 runs): for a partition that stops receiving writes the row was queryable after 34.2 to 38.2 s with the default `local.retention.ms=30000`, so a guarantee below 34.2 s did not hold in any of these 3 runs, and one below 38.2 s did not hold in at least one. **inference**: what closed the segment is the idle roll (see above), so a guarantee much tighter than about 40 s cannot be expected for quiet partitions in this setup unless `local.retention.ms` is lowered (**not tested**) or a periodic writer touches every partition.

## Experiment 7: restarting a leader while segments are uploaded

### Method

`scripts/verify/stage2-duplicates.sh` (settings printed at the top of every transcript):

1. `make clean`, `make up STAGE=iceberg`, `SEGMENT_BYTES=262144 make topic` (small segments, so uploads are frequent; the rest is the lab default, `local.retention.ms=30000`).
2. `make -s batch N=100000 PACE=2000 LAB_KEY_FIELD=customer_id` in the background (paced, because an unthrottled batch into 256 KiB segments fails with `RECORD_LIST_TOO_LARGE`, see `02-stage2-iceberg.md`). Avro keys are the lab default in stage 2.
3. Wait until the leader of partition 0 has logged one `Copied ... events-0` (so it has uploaded at least one segment before the stop; the transcript prints the table, snapshots and Parquet files at that moment), then wait for its next `Copying ... events-0` line and restart that broker at once: `docker compose restart kafka-<leader>` (runs 1 and 3, graceful) or `docker compose kill -s KILL` then `start` (run 2).
4. Wait for full ISR, produce a kicker batch (`N=3000 START=100000`), wait until the table holds as many rows as the brokers' end offsets, then print snapshots and, per Parquet file in the bucket, the partition and offset range (read as plain Parquet).
5. **A**: `python -m lab.pairs`: the table's `(partition, offset)` pairs against a replay from offset 0 to the end offsets taken at the start (all duplicates kept) against the broker end offsets, plus the record (`order_id`) found at each offset. Also `make replay`.
6. **B**: `make broker-stop` of the broker that leads partition 0 now, wait for a new leader, run the same check. Then the key query.

`lab/pairs.py` unit tests: `tests/test_pairs.py`.

### The restart hit an upload in flight in all three runs

**Verified** (the `## upload lines of events-0` section of each transcript): the old leader's last line for partition 0 is a `Copying` with no `Copied`, and the restart or kill came about 40 ms after it:

| Run | Mode | Old leader | `Copying` at (broker clock) | Restart at (script clock, UTC) | New leader's log |
|---|---|---|---|---|---|
| 1 | graceful | broker 2 | `...3428.log` 14:34:30.545 | 14:34:30.588 | broker 3: `Found the highest copiedRemoteOffset: ...offset=3427...` 14:34:31.073, `Copying ...3428.log` 14:34:31.074, `Copied` 14:34:32.406 |
| 2 | SIGKILL | broker 1 | `...3422.log` 14:46:12.515 | 14:46:12.551 | broker 2: `...offset=3421...` 14:46:22.159, `Copying ...3422.log` 14:46:22.160, `Copied` 14:46:23.171 |
| 3 | graceful | broker 1 | `...3422.log` 15:01:05.946 | 15:01:05.987 | broker 2: `...offset=3421...` 15:01:06.184, `Copying ...3422.log` 15:01:06.185, `Copied` 15:01:07.502 |

So the new leader started from the last offset it knew was copied and copied the same segment again (same base offset) in every run. No `ERROR` or `WARN` line from `RemoteLogManager`, `RLMCopyTask`, `RemoteStorage` or Iceberg was logged in the three transcripts' brokers (the grep section of each transcript is empty). The old leader had uploaded one segment of partition 0 (base offset 0) before the restart in all runs (the "state before the restart" section: 1 Parquet file per partition, 0 overlapping, 0 not in metadata).

### Results per run

**Verified**, all from the transcripts (table side is `iceberg_scan`, i.e. what a query sees):

| | Run 1 (graceful) | Run 2 (SIGKILL) | Run 3 (graceful) |
|---|---|---|---|
| Producer, first batch | produced 100000, failed 0 | produced 94962, failed 5038 | produced 100000, failed 0 |
| Broker end offsets (sum) | 31218 + 34609 + 37173 = 103000 | 26180 + 34609 + 37173 = 97962 | 103000 |
| Table rows | 103000 | 97962 | 103000 |
| Table duplicate `(partition, offset)` pairs | 0 | 0 | 0 |
| Table missing pairs | 0 | 0 | 0 |
| Partition 0 rows in all Parquet files in the bucket, against distinct offsets | 34636 against 31218 (+3418) | 26180 against 26180 | 34638 against 31218 (+3420) |
| Overlapping Parquet files of the same partition | 1 | 0 | 1 |
| Parquet files in the bucket that the current metadata does not list | 1 | 0 | 1 |
| Snapshots at the end | 34 (2 empty + 32 with `added-data-files=1`) | 32 (2 empty + 30; 30 Parquet files in the bucket) | 33 (1 empty + 32) |

- **verified** (runs 1 and 3): the same offset range of partition 0 sits in two Parquet files (run 1: offsets 3428 to 6845, 3418 rows, in two files; run 3: offsets 3422 to 6841, 3420 rows, in two files). One of the two is not in the current Iceberg metadata, and the table has no duplicate. So the interrupted upload left an **orphan data file** in the `warehouse` bucket. **inference**: the committed file is the retry's; the listing shows no file name, modification time or writer, so which of the two was written first is not shown. The count of commits matches the files in the metadata: run 1 has 33 files in the bucket, 1 not in metadata, 32 snapshots with a data file; run 3 has 33, 1 and 32.
- **verified**: no extra snapshot came from the restart. The snapshot count equals the number of files in the metadata (plus the empty snapshots the table creation writes, 2 in runs 1 and 2 and 1 in run 3, a variation I did not investigate).
- **inference**: the old leader wrote the Parquet file but did not finish the commit and the remote-log metadata update before it stopped, so the new leader saw the segment as not copied and uploaded it again. That the first file was complete is not shown (only its row count, 3418 or 3420, equal to the second file's). In run 2 the kill came while the copy had just started, and the bucket holds no orphan: the old leader's file was either never written or never completed; **not tested** which.
- **verified**: the table is complete and duplicate-free after the restart in the three runs. **not tested**: restarts at other moments (for example between the Parquet write and the commit, if the plugin exposes such a moment), repeated restarts, a stopped broker that stays down, other partitions, the catalog restarting, a restart under unthrottled load.
- **verified, unrelated to the table** (run 2): after `SIGKILL`, the paced producer reported `failed: 5038` of 100000 (`delivery failures logged by the producer: 5038`). The reason strings were not kept in the transcript, so the cause is **not tested**. The pairs check compares against the broker end offsets, so these records are simply not in the topic.
- **not shown** (no committed transcript): an aborted first attempt of this experiment logged one `ERROR ... Error occurred while copying log segments` on kafka-2 for `events-2` after a graceful restart; that transcript was not kept, so the error is unevidenced here. The three committed runs show no ERROR or WARN from the remote-log copy path.

### Replay through the new leader: what one client received

A and B in each run gave the same numbers. The leaders were: run 1: A broker 3, B broker 2 (the old leader, which wrote the first segment); run 2: A broker 2, B broker 1; run 3: A broker 2, B broker 1. In every A the leader of partition 0 was a broker that had written none of the segments uploaded before the restart (the old leader had); in B it was the original leader again, which wrote the first segment. Local segments were empty on all three brokers at the time (`make ls-local` section: all 9 files have size 0), so the history was served from tiered storage.

**Verified for one client**, the lab's `lab.pairs` on confluent-kafka 2.16.0 (librdkafka 2.16.0) (`## A)` and `## B)` sections; `replay_rows`, `replay_duplicates` and `replay_missing` are counted per `(partition, offset)` against the broker end offsets). The next section corroborates what this client received at the broker level, with Kafka's own consumer and raw Fetch requests:

| | Run 1 | Run 2 | Run 3 |
|---|---|---|---|
| Records the brokers hold | 103000 | 97962 | 103000 |
| Pairs delivered by the replay | 102416 | 97968 | 102832 |
| Repeated pairs (duplicates) | 2640 | 2479 | 2853 |
| Offsets never delivered (missing) | 3224 | 2473 | 3021 |
| Offsets that carry a different record in the replay than in the table | 97636 | 88598 | 97828 |
| Replay status | stalled: no record for 30 s | stalled | stalled |
| A and B identical? | yes | yes | yes |

- **verified** (that client): the replay stalls before the end offsets in every run (run 1: partition 0 last offset 30795 of 31218, partition 1 34425 of 34609, partition 2 37164 of 37173; the progress line is in the transcript), and the existing `make replay` fails the same way: `replay failed: read 99776 of 103000 records in 120.0s` (run 1), `read 95489 of 97962` (run 2), `read 99979 of 103000` (run 3).
- **verified** (that client): the table is the correct side: in `stage2-replay-source-256k-paced.txt` the same topic, replayed while every segment was still on the brokers' disks (`local.retention.ms=3600000`), gives 20000 pairs, 0 duplicates, 0 missing and 0 offsets with a different record against the table (the table then held 13779 of the 20000 rows, as the open segments were not uploaded yet; the comparison covers the offsets present). After `local.retention.ms` was set to 30000 and the local segments were deleted, the replay of the same topic gave 20002 pairs, 580 duplicates, 578 missing and 18841 offsets with a different record; the table was complete (20000 rows, 0 duplicates, 0 missing).
- **verified, one run per arm** (that client): `stage2-replay-source-1m-unthrottled.txt` (default 1 MiB segments, `make batch N=20000` unthrottled, customer_id keys): the tiered replay is complete, 20000 pairs, 0 duplicates, 0 missing, 0 different records. `stage2-replay-source-1m-paced.txt` (default segments, `PACE=2000`): tiered replay 20000 pairs, 527 duplicates, 527 missing, 19044 different records. In both 1 MiB runs the local-replay step has no result (the table did not exist yet, because all data was still in open segments, and `lab.pairs` then failed as a whole; it now checks the replay without the table), so the local baseline comes from the 256 KiB run only. **inference** at the time: the difference comes from how the data was produced. **Correction (Task 13b)**: what matters is the producer batch structure, not pacing. Both transcripts can be right, but the generalisation "unthrottled is exact" is refuted. `stage2-replay-kafka-tools-customer-keys-unthrottled.txt` repeats that exact run's configuration on the same data (end offsets 6066, 6727, 7207 again). It was wrong in partitions 0 and 1 (partition 0's segment held 7 producer batches; 969 and 1075 repeated and missing pairs) and exact in partition 2, which came back as one batch. Unthrottled batching also differs between runs of one configuration: `stage2-replay-kafka-tools-unthrottled.txt` (order_id keys, unthrottled) as first committed in ab85cc1 (`git show ab85cc1:docs/findings/evidence/stage2-replay-kafka-tools-unthrottled.txt`) held 7 producer batches in partition 0, and its tiered replay had 1109 repeated and 1109 missing pairs in partition 0 (3242 and 3242 in all). The regenerated transcript of the same configuration, which overwrote it in commit 81e1681, held one producer batch in partition 0, which was exact. **inference**: the earlier exact run had a batch structure where no rebuilt batch started at the last record of a multi-record producer batch, for example one producer batch per partition. Its transcript has no batch counts, so why it was exact is not shown. **inference**, consistent with different batching: its partition 0 segment file was 437016 bytes (`make ls-local` in `stage2-replay-source-1m-unthrottled.txt`, the earlier exact run, not in the ab85cc1 transcript), against 433793 bytes for the same 6066 records in the customer-keys rerun (position plus size of the last of its 7 batches in the dump).
- **inference**: the paced runs produce many small producer batches (about 40 records at 2000 events/s with `linger.ms=20`) and the unthrottled run few large ones (`stage2-batch-fields.txt` in `02-stage2-iceberg.md` shows about 1000 records per batch there); together with the earlier finding that the last record of a batch carries a different `batch_byte_offset`, this points at the plugin rebuilding Kafka batches from Parquet rows wrongly when there are many batches. I did not measure the batch sizes in these runs and did not test the reconstruction directly, so this is **not tested**. **Superseded (Task 13b)**: the reconstruction was then tested directly; the next section replaces this inference. The estimate of about 40 records per paced producer batch was also too high: the Task 13b paced dump holds 6569 records of partition 0 in 659 batches, about 10 per batch.

**inference**, on the leader question: the history is served identically by the broker that wrote it and by one that did not (A against B, three runs), and wrongly by both. In the Task 13b runs that happened when a segment held several producer batches and a rebuilt batch started at the last record of one that held more than one record. So a leader change does not lose or duplicate history in the table, but these runs cannot say it keeps the history intact for consumers, because the consumer path is already wrong without any leader change.

### Independent check at the broker level (Task 13b)

The replay numbers above are what one client received. They were rechecked without that client by `scripts/verify/stage2-replay-kafka-tools.sh`, each run on a fresh stack. Scope of everything in this section: Kafka 4.0.0, plugin 1.1.1 in Iceberg mode (alpha), 3 partitions, one idempotent producer, uncompressed batches (compression 0 in every returned batch), the first tiered segment of each partition (base offset 0), raw Fetch version 4. Five transcripts, one per run:

- `stage2-replay-kafka-tools-paced.txt`: `make batch N=20000 PACE=2000`, order_id keys;
- `-one-record-batches-20k.txt`: the same batch with the producer set to one record per batch (`LAB_BATCH_NUM_MESSAGES=1`, librdkafka `batch.num.messages=1`);
- `-unthrottled.txt`: `N=20000`, unthrottled, order_id keys;
- `-customer-keys-unthrottled.txt`: the configuration of `stage2-replay-source-1m-unthrottled.txt` (customer_id keys, unthrottled, default 1 MiB segments, `local.retention.ms=3600000` then 30000). Line 1 of this transcript reads `local.retention.ms=3600000, later 300003600000`, from a header bug in the script (since fixed). The topic had `local.retention.ms=3600000` first and 30000 later, as its `Configs:` lines and its `kafka-configs --alter ... local.retention.ms=30000` line show;
- `-one-record-batches.txt`: `N=200 PACE=20`: records 50 ms apart with `linger.ms=20`, so most producer batches hold one record.

The ground truth is partition 0's original segment and its offset index, dumped with `kafka-dump-log.sh` on the leader before the upload and the local deletion (the dump holds the whole partition in all five runs; the script would print a warning otherwise). Records are named by the `order_id` in the value, unique per event, so customer_id keys do not matter.

**Verified, the table against the original segment** (partition 0, all five runs): the key, `value.order_id` and `kafka.batch_base_offset` equal the original record and its batch base offset at every offset (6569, 6569, 6569, 6066 and 66 rows). `kafka.batch_byte_offset` does not: it equals the position of the record's own producer batch on 495 of 6569, 216 of 6569, 6569 of 6569 (one producer batch), 639 of 6066 and 11 of 66 rows (the first Task 13b round, commit ab85cc1, had 245 of 6569 in its paced run). On every row of every run it equals what a lookup in the segment's offset index returns for the record's offset (`index_vs_table.py`). The source reading below shows why.

**Verified, the local log is served right**: Kafka's `kafka-console-consumer.sh` on partition 0, while the segment was still on disk, delivered every offset once with the original record (6569, 6569, 6569, 6066 and 66 records, 0 differences).

**Verified, what the broker returns for the tiered history**, with Kafka's own consumer and with raw Fetch requests (no client library: `scripts/verify/rawfetch.py` sends Fetch v4 over a TCP socket and decodes RecordBatch v2):

| Partition 0, history from tiered storage | Paced | One record per producer batch | Unthrottled | Customer keys, unthrottled | 200 records, 50 ms apart |
|---|---|---|---|---|---|
| Producer batches in the original segment | 659 | 6569 | 1 | 7 | 58 |
| Offset-index entries | 113 | 215 | 1 (empty index) | 6 | 2 |
| Batches the leader returns for a Fetch at offset 0 (one response) | 114 | 216 | 1 | 7 | 3 |
| Returned batches whose offsets differ from the table's | 113 (shift up to 14) | 0 | 0 | 6 (shift up to 968) | 0 |
| Last offset the returned batches reach (end offset) | 6560 (6569) | 6568 (6569) | 6568 (6569) | 5323 (6066) | 65 (66) |
| `kafka-console-consumer.sh` from offset 0: records delivered | 6407 of 6569 | 6569 of 6569 | 6569 of 6569 | 5097 of 6066 | 66 of 66 |
| ... offsets missing between the first and last delivered | 154 | 0 | 0 | 227 | 0 |
| ... records whose `order_id` differs from the table's at that offset | 6018 | 0 | 0 | 3548 | 0 |
| All partitions, every returned batch: CRC32C valid | 349 of 349 | 656 of 656 | 17 of 17 (*) | 15 of 15 | 8 of 8 |
| `lab.pairs` on the same data: repeated / missing pairs, all partitions | 514 / 514 | 0 / 0 | 1784 / 2 | 2044 / 2044 | 0 / 0 |

(*) Partitions 1 and 2 of the unthrottled run returned the same two batches to four Fetch requests (see below); every copy is counted.

- **verified**: every returned batch holds records with contiguous true offsets in the true order, with `offsetDelta` equal to the record index (every batch, `all returned batches` lines), and a `baseOffset` equal to the table's `batch_base_offset` of the batch's first record (partition 0: 114 of 114, 216 of 216, 1 of 1, 7 of 7, 3 of 3). Each returned batch is exactly one run of rows sharing a `batch_byte_offset` value (same counts). Against the original segment, each returned batch after the first starts at the last record of a producer batch (113 of 113, 6 of 6; also in the two runs with one-record producer batches, where that record is the whole producer batch). So (arithmetic from these facts) every record of such a batch is labelled lower than its true offset by the size of that producer batch minus one: 0 when producer batches hold one record (the one-record-per-batch run: 0 of 216 returned batches shifted), up to 14 in the paced run and up to 968 in the customer-keys run (partition 0; partitions 1 and 2 reached 14 and 16 in the paced run, 1069 and 0 in the customer-keys run). Partitions 1 and 2: paced 118 of 119 and 115 of 116 returned batches shifted, one-record-per-batch 0 and 0, customer keys 6 of 7 and 0 of 1.
- **verified**: the labels overlap and leave gaps (paced, partition 0: 52 returned batches start at or below the previous batch's last offset, 50 leave a gap). `kafka-console-consumer.sh` (Kafka 4.0.0, the Java consumer) did not deliver 162 records of partition 0 in the paced run (6407 of 6569) and 969 in the customer-keys run (5097 of 6066), and delivered the rest under wrong offsets. **inference**: the records it left out are those labelled below its position (their count equals the repeated offset labels in the raw Fetch responses). librdkafka delivered every record, with repeated and missing offsets. In the paced, customer-keys and both one-record runs, `lab.pairs`' repeated and missing counts per partition equal the repeated and unused offset labels in the raw Fetch responses (paced 162, 179, 173; customer keys 969, 1075, 0; one-record runs 0), so the lab's tool reported what the broker sent. The unthrottled run differs. In partitions 1 and 2 the response ends with a one-record batch labelled below the batch before it (partition 1: a batch labelled 0 to 6783, then one labelled 5951). A Fetch at the next offset after that last batch (5952) returns the same two batches again. `rawfetch.py --walk 3` fetches at the last offset returned plus one by construction, so the four identical responses in the transcript are one fetch and three repeated re-fetches. librdkafka delivered records again (833 and 951 repeated pairs, 1 missing each; 832 and 950 repeated `order_id`s, 1782 in all), more than the labels of one response (1 repeated and 1 unused). This behaviour is not fully characterised. **inference**, from Kafka's `findFirstBatch` (source section below) and the counts: after the first response librdkafka fetched at 5952. `findFirstBatch(5952)` returns the first batch whose `lastOffset` is at least 5952, the batch labelled 0 to 6783, so the same two batches came back. librdkafka delivered the 832 records labelled 5952 to 6783 again (the same records, hence 832 repeated `order_id`s), did not deliver the one labelled 5951 again, and stopped at position 6784 (the recorded consumer position), where no batch qualifies. That gives 6784 + 1 + 832 = 7617 records and 833 repeated labels (5951 to 6783) in partition 1, and 6645 + 1 + 950 = 7596 records and 951 repeated labels in partition 2, as measured. Kafka's Java consumer was not tried on these partitions (**not tested**).
- **verified, the stall**: a Fetch at the offset after the last returned label (paced 6561, customer keys 5324) returns error `NONE`, 0 bytes and no batch, three times 20 s apart and again after 300 more records were produced; those new records are served from the old end offset (6569, 6066). A consumer that reached that offset got nothing more in these runs, while no later segment was tiered (`findFirstBatch` then tries the next remote segment, see the source section; recovery **not tested**). This is the broker's answer, not a client timeout. Where the last rebuilt batch is not shifted (both one-record runs, unthrottled partition 0) there is no stall.
- **verified** (`## d)` sections): the partition-0 leader logs no WARN, ERROR or exception during these reads. Besides the remote-log-manager copy-task DEBUG lines and the local segment deletions, the reads show only as `kafka.log.remote.RemoteLogReader` DEBUG lines; no `io.aiven` logger line and no "Time taken" line appear. The plugin's own INFO line `fetchLogSegment from position {}` (source below) does not appear either; why is **not tested**.
- **not tested**: compressed or transactional batches, several producers, other plugin versions, Fetch versions other than 4, tiered segments with a non-zero base offset (see the read filter below), and whether a consumer stuck at the stall offset recovers once a later segment is tiered.

### What the source says: write side and read side (Task 13b, read-only)

Read from `Aiven-Open/tiered-storage-for-apache-kafka` at tag `v1.1.1` (commit `1b294d7`) and `apache/kafka` at tag `4.0.0`; line numbers are those tags'.

- **Write side** (`core/src/main/java/io/aiven/kafka/tieredstorage/IcebergRemoteStorageManager.java`): `copyLogSegmentData` opens the segment's offset index (line 184, `new OffsetIndex(logSegmentData.offsetIndex().toFile(), remoteLogSegmentMetadata.startOffset())`), loops `for (final var batch : segment.log().batches())` / `for (final Record record : batch)` (lines 199-200) and calls `writeRecordToIceberg`, which writes `kafkaPart.put(RowSchema.Fields.BATCH_BYTE_OFFSET, offsetIndex.lookup(parsedRecord.recordOffset()).position);` (line 306) and `kafkaPart.put(RowSchema.Fields.BATCH_BASE_OFFSET, batch.baseOffset());` (line 307). In Kafka, `OffsetIndex.lookup` (`storage/src/main/java/org/apache/kafka/storage/internals/log/OffsetIndex.java`, lines 88-106) finds "the largest offset less than or equal to the given targetOffset", or returns `(baseOffset, 0)` when there is none. `LogSegment.append` (`.../log/LogSegment.java`, lines 261-262) adds an entry `offsetIndex().append(largestOffset, physicalPosition)` only `if (bytesSinceLastIndexEntry > indexIntervalBytes)`, keyed by the last offset of the appended records at the position where they start. The customer-keys transcript shows such entries, for example `offset: 1549 position: 45249` for the producer batch 633 to 1549 at position 45249. So, as the v1.1.1 write code computes it, `batch_byte_offset` is the position of the latest index entry whose offset (the last offset of an indexed append) is at or below the record's offset, not the position of the record's own batch. The measured agreement on every row is that value. **inference**: it is what this code writes, not a corruption at upload.
- **Read side**: `fetchLogSegment` (same file, line 365) reads the segment's Parquet rows with the filter `Expressions.greaterThanOrEqual("kafka.batch_byte_offset", remoteLogSegmentMetadata.startOffset())` (lines 399-400). It compares a byte position with an offset; the requested start position is only logged (line 368, with `// TODO read only files that contain startPosition`, line 376). `RecordBatchGrouper.nextBatch` (`core/src/main/java/io/aiven/kafka/tieredstorage/iceberg/RecordBatchGrouper.java`, lines 42-57; class comment "Groups records in batches based on the `kafka.batch_byte_offset` field") collects rows while `batchByteOffset(peekNext()) == currentBatchOffset`. `BatchEnumeration` (`.../iceberg/BatchEnumeration.java`, lines 89-118) builds each Kafka batch as `new MemoryRecordsBuilder(os, ..., getBatchBaseOffset(firstRecord), ..., getBaseSequence(firstRecord), ...)` and adds every row with `builder.append(new SimpleRecord(...))`. Kafka's `MemoryRecordsBuilder.append(SimpleRecord)` uses `nextSequentialOffset()`, that is `baseOffset`, `baseOffset + 1`, ... (`clients/src/main/java/org/apache/kafka/common/record/MemoryRecordsBuilder.java`, lines 602-604 and 907-909). The plugin's unit test `RecordBatchGrouperTest.multipleBatches` models one `batch_byte_offset` value per batch.
- **The stall** in Kafka 4.0.0: `RemoteLogManager.read` (`core/src/main/java/kafka/log/remote/RemoteLogManager.java`, lines 1696-1711) calls `findFirstBatch` (lines 1902-1914). That method skips batches `while (nextBatch != null && nextBatch.lastOffset() < offset)`; when no batch in the segment qualifies it tries the next remote segment, and with none it returns `MemoryRecords.EMPTY`. That matches the empty responses above.
- **inference**: the measurements agree with the v1.1.1 code, read as follows. That this code path ran is shown only by the matching numbers: the plugin's own INFO line `fetchLogSegment from position {}` never appears in the broker logs. The code: one rebuilt batch per run of equal `batch_byte_offset`, base offset from the run's first row, record offsets base + index. The shift appears when the two sides meet. The write side stores an index-interval position (as written, line 306). The read side treats a run of equal values as one batch (as written, `RecordBatchGrouper`). A run starts at the last record of an indexed producer batch, whose `batch_base_offset` is that producer batch's first offset. **inference** (intent is not in the code): the field name, the grouper's comment and its test treat `batch_byte_offset` as a batch identifier, so the write side's value is the one that departs from that meaning. Which side the authors consider wrong is not established. **not tested**: the read filter on segments with a non-zero base offset; every segment checked here started at offset 0, where it filters nothing.

**Verdict, calibrated**: supported. Under the scope above, the broker returns tiered history in batches whose `baseOffset` disagrees with the table and with the original segment. This held for every returned batch after the first that starts at the last record of a producer batch of more than one record (paced partition 0: 113 of 113; customer keys: 6 of 6), and for none when producer batches held one record (0 of 216). The table's keys, `order_id`s and `batch_base_offset` match the original segment at every offset; its `batch_byte_offset` does not, as the v1.1.1 write code computes it. The lab's tool reported what the broker sent.

### Key to partition after a leader change

**Verified** (`## key mapping` sections in all three transcripts): with `LAB_KEY_FIELD=customer_id` (500 distinct customers, key equal to `value.customer_id` in every row: `rows_where_key_differs_from_customer_id` 0), `SELECT value.customer_id AS customer_id, count(DISTINCT kafka.partition) AS partitions FROM {table} GROUP BY 1 HAVING count(DISTINCT kafka.partition) > 1` returned no row before the restart (partial table), after the restart (A) and after the stop of the new leader (B), in each run (row counts 103000, 97962, 103000). A query on a top-level `customer_id` would fail: the column is `value.customer_id`. **inference**: the guarantee is the producer's key hash over a fixed partition count, which a leader change does not touch. **not tested**: a change of the partition count. The query after B reads the same table as the query after A: no record is produced between A and B (`stage2-duplicates.sh`), so B repeats A rather than testing the stop.

### What this means for an ingestion service

- **inference** from the three runs: a service that reads the Iceberg table can rely on exactly-once rows through a leader restart (0 duplicates, 0 missing against the brokers' end offsets in each run). It must expect orphan Parquet files in the warehouse after an interrupted upload (1 file of 3418 or 3420 rows per hit run), so it needs a cleanup of files the table does not reference and must not count bucket files as data.
- **inference**, within the Task 13b scope (Kafka 4.0.0, plugin 1.1.1, uncompressed batches, one producer, the partitions tested): a service that replays the topic in Iceberg mode cannot trust the offsets of tiered history when a segment holds several producer batches and a rebuilt batch starts at the last record of one that holds more than one record (whether that happens depends on how the producer batched: the paced and customer-keys runs, not the one-record runs). In the `lab.pairs` runs of this document the repeated or missing pairs per run were 2473 to 3224 in experiment 7 and 514/514, 2044/2044 and 1784/2 (repeated/missing) in the Task 13b runs where it went wrong. `kafka-console-consumer.sh` (Kafka 4.0.0) did not deliver some records (162 of 6569 in partition 0 of the paced Task 13b run). A consumer stopped short of the end offset in these runs when the last rebuilt batch was shifted. Such a service should read history from the table, key its de-duplication on a field of the record such as `order_id`, and not rely on the topic replay with this plugin version.

## Not tested (summary)

- Restart or kill at moments other than "just after `Copying` started"; a leader that stays down; the catalog or MinIO restarting during an upload; unthrottled load during the restart.
- Newer plugin versions; compressed or transactional batches; several producers; Fetch versions other than 4; tiered segments with a non-zero base offset (the plugin's read filter compares `batch_byte_offset` with the segment's start offset); whether a consumer stuck at the stall offset recovers once a later segment is tiered; Kafka's Java consumer on the repeated re-fetch of the unthrottled run's partitions 1 and 2.
- `segment.ms` below 5000, other `local.retention.ms` values in the idle case, other broker check intervals, higher rates and many partitions for the freshness numbers.
- The cause of the 5038 failed deliveries in the SIGKILL run.
