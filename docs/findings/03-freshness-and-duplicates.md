# Stage 2: freshness (experiment 6) and duplicates (experiment 7)

Every number below is copied from a transcript in `docs/findings/evidence/` (named in the text), produced by a script in `scripts/verify/`. Each statement carries a label: **verified** (shown by the named transcript), **inference** (my reading of verified facts, not shown directly) or **not tested**. All runs used 3 brokers, topic `events` with 3 partitions and replication factor 3, plugin 1.1.1 in Iceberg mode and the `apache/iceberg-rest-fixture:1.10.0` catalog, each from `make clean && make up STAGE=iceberg`. Times in broker log lines are UTC (the broker clock); the harness converts its own times to UTC too, so the two are comparable to about 0.1 s (a "Copied" line can be timed up to 0.04 s after the row was already seen, see below).

Transcripts and the scripts that wrote them:

| Transcript | Script | What |
|---|---|---|
| `stage2-freshness.txt` | `scripts/verify/stage2-freshness.sh` (uses `lab/freshness.py`) | experiment 6, 13 runs |
| `stage2-duplicates-1.txt`, `-2.txt`, `-3.txt` | `scripts/verify/stage2-duplicates.sh` (uses `lab/pairs.py`) | experiment 7, three runs |
| `stage2-replay-source-256k-paced.txt`, `-1m-unthrottled.txt`, `-1m-paced.txt` | `scripts/verify/stage2-replay-source.sh` (uses `lab/pairs.py`) | control for the replay results of experiment 7 |
| `stage2-replay-kafka-tools-paced.txt`, `-unthrottled.txt`, `-one-record-batches.txt` | `scripts/verify/stage2-replay-kafka-tools.sh` (Kafka's own `kafka-dump-log.sh` and `kafka-console-consumer.sh`, raw Fetch requests, no lab client) | independent check of the replay result (Task 13b) |

## Experiment 6: how long until a produced record is queryable

### Method

- `lab/freshness.py` produces one marker event (Avro key and value, order id `marker-<8 hex>`), starts the clock when the broker acknowledged it, then (when filler is on) produces one filler event per second and, once a second, asks the catalog for the table and counts rows with `value.order_id = '<marker>'` through DuckDB (a new connection per check). It prints one JSON line with `seconds_to_queryable`, the partition and offset the marker landed in, and UTC timestamps.
- Cases (`stage2-freshness.sh`, 3 runs of each; the last case 1 run), each from a clean stack and a recreated topic:
  - `seg5000`, `seg15000`, `seg60000`: `SEGMENT_MS=<ms> SEGMENT_BYTES=104857600 LOCAL_RETENTION_MS=30000 make topic`, then `uv run python -m lab.freshness --segment-ms <ms> --filler-interval 1 --poll 2 --timeout 300`.
  - `seg60000-lrms600000` (1 run): as `seg60000` but `LOCAL_RETENTION_MS=600000`.
  - `control`: the lab's default topic (`LOCAL_RETENTION_MS=30000`, `segment.bytes=1048576`, `segment.ms` not set; the transcript shows the broker default `segment.ms=604800000`), one marker and **no filler** (`--filler-interval 0`).
- What the setting under test is, and what stayed constant: **verified** (`stage2-freshness.txt`, the `kafka-configs --describe` lines of every run): in the three `segment.ms` cases `segment.bytes` was 104857600, so the size limit cannot close the segment in these runs (the marker plus about 60 fillers are a few kB); `local.retention.ms` was 30000 (the lab default) except in `seg60000-lrms600000`; the 5 s remote-log-manager interval was not changed (broker default, **not tested** at other values); the harness checks the table about once a second; one producer, tiny rows, no other load. The three partitions each got about a third of the fillers (random keys), so a given partition sees a write about every 3 s.

### Headline table

Seconds from marker acknowledged to marker row returned by the query (`stage2-freshness.txt`, `## summary` and the JSON lines). **verified**.

| Setting | Runs | Seconds to queryable | Range |
|---|---|---|---|
| `segment.ms=5000` (filler) | 3 | 13.49, 7.24, 8.55 | 7.24 to 13.49 |
| `segment.ms=15000` (filler) | 3 | 20.86, 22.76, 22.57 | 20.86 to 22.76 |
| `segment.ms=60000` (filler) | 3 | 64.29, 72.27, 60.96 | 60.96 to 72.27 |
| `segment.ms=60000`, `local.retention.ms=600000` (filler) | 1 | 62.82 | |
| default topic, single marker, no filler (control) | 3 | 34.23, 38.20, 34.22 | 34.22 to 38.20 |

No run timed out or failed (the transcript has no `exit code` line). The number of fillers sent equals the seconds waited rounded up (for example 14 fillers for 13.49 s).

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
- **verified**: without any further write (control), the marker's segment rolled 30.3 to 31.9 s after the marker and the row was queryable after 34.2 to 38.2 s. **inference**: the roll is the idle roll about `local.retention.ms` (30000) after the last write, plus the broker's periodic check (the 5 s interval is the broker default; I did not vary it or `local.retention.ms`, so the dependence is **not tested**). It agrees with the 35 to 40 s measured in `docs/findings/02-stage2-iceberg.md`.
- **inference**, not a formula: the results are consistent with `delay = time until the segment rolls + 0 to 5 s until the next upload cycle + about 1 s copy + at most about 1 s until my check`. The time until the segment rolls is `segment.ms` plus up to about 11 s of slack in my runs when writes keep arriving, and about `local.retention.ms` when they do not. The slack with `segment.ms` is consistent with Kafka evaluating the roll when a record is appended to that partition (the topic note in `lab/freshness.py`): the fillers reach a given partition about every 3 s on average, and the largest slack (+11.2 s in `seg60000` #2, +6.3 s in `seg5000` #1) is more than that, so I cannot say what else was waiting. **not tested**: any `segment.ms` below 5000, any other `local.retention.ms` in the idle case, any broker interval, higher rates, many partitions rolling at once, larger rows.
- **verified, side effect of the design**: the harness sees the row only after the whole segment is uploaded, so one visible row means all rows of that segment before it are visible too.

### What this means for an ingestion service with a "queryable within N seconds" guarantee

- Under steady writes to every partition, the observed delay was about `segment.ms` plus 1 to 12 s (7.2 to 13.5 s at 5000, 20.9 to 22.8 s at 15000, 61.0 to 72.3 s at 60000, 3 runs each), so a guarantee of N seconds needs `segment.ms` of at least 5 s and at most roughly N minus 15 s in this setup; I measured no value below 5000.
- For a partition that stops receiving writes the only thing that closes its segment in the runs here is the idle roll, which gave 34.2 to 38.2 s with the default `local.retention.ms=30000`, so a guarantee tighter than about 40 s cannot hold for quiet partitions unless `local.retention.ms` is lowered (**not tested**) or a periodic writer touches every partition.

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

- **verified** (runs 1 and 3): the same offset range of partition 0 sits in two Parquet files (run 1: offsets 3428 to 6845, 3418 rows, in two files; run 3: offsets 3422 to 6841, 3420 rows, in two files). One of the two is not in the current Iceberg metadata, and the table has no duplicate. So the interrupted upload left an **orphan data file** in the `warehouse` bucket, and the retry's file was the one committed. The count of commits matches the files in the metadata: run 1 has 33 files in the bucket, 1 not in metadata, 32 snapshots with a data file; run 3 has 33, 1 and 32.
- **verified**: no extra snapshot came from the restart. The snapshot count equals the number of files in the metadata (plus the empty snapshots the table creation writes, 2 in runs 1 and 2 and 1 in run 3, a variation I did not investigate).
- **inference**: the old leader wrote the Parquet file but did not finish the commit and the remote-log metadata update before it stopped, so the new leader saw the segment as not copied and uploaded it again. That the first file was complete is not shown (only its row count, 3418 or 3420, equal to the second file's). In run 2 the kill came while the copy had just started, and the bucket holds no orphan: the old leader's file was either never written or never completed; **not tested** which.
- **verified**: the table is complete and duplicate-free after the restart in the three runs. **not tested**: restarts at other moments (for example between the Parquet write and the commit, if the plugin exposes such a moment), repeated restarts, a stopped broker that stays down, other partitions, the catalog restarting, a restart under unthrottled load.
- **verified, unrelated to the table** (run 2): after `SIGKILL`, the paced producer reported `failed: 5038` of 100000 (`delivery failures logged by the producer: 5038`). The reason strings were not kept in the transcript, so the cause is **not tested**. The pairs check compares against the broker end offsets, so these records are simply not in the topic.

### Replay through the new leader: the broker serves the tiered history wrongly in these runs

A and B in each run gave the same numbers. The leaders were: run 1: A broker 3, B broker 2 (the old leader, which wrote the first segment); run 2: A broker 2, B broker 1; run 3: A broker 2, B broker 1. In every A the leader of partition 0 was a broker that had written none of the segments uploaded before the restart (the old leader had); in B it was the original leader again, which wrote the first segment. Local segments were empty on all three brokers at the time (`make ls-local` section: all 9 files have size 0), so the history was served from tiered storage.

**Verified** (`## A)` and `## B)` sections; `replay_rows`, `replay_duplicates` and `replay_missing` are counted per `(partition, offset)` against the broker end offsets):

| | Run 1 | Run 2 | Run 3 |
|---|---|---|---|
| Records the brokers hold | 103000 | 97962 | 103000 |
| Pairs delivered by the replay | 102416 | 97968 | 102832 |
| Repeated pairs (duplicates) | 2640 | 2479 | 2853 |
| Offsets never delivered (missing) | 3224 | 2473 | 3021 |
| Offsets that carry a different record in the replay than in the table | 97636 | 88598 | 97828 |
| Replay status | stalled: no record for 30 s | stalled | stalled |
| A and B identical? | yes | yes | yes |

- **verified**: the replay stalls before the end offsets in every run (run 1: partition 0 last offset 30795 of 31218, partition 1 34425 of 34609, partition 2 37164 of 37173; the progress line is in the transcript), and the existing `make replay` fails the same way: `replay failed: read 99776 of 103000 records in 120.0s` (run 1), `read 95489 of 97962` (run 2), `read 99979 of 103000` (run 3).
- **verified**: the table is the correct side: in `stage2-replay-source-256k-paced.txt` the same topic, replayed while every segment was still on the brokers' disks (`local.retention.ms=3600000`), gives 20000 pairs, 0 duplicates, 0 missing and 0 offsets with a different record against the table (the table then held 13779 of the 20000 rows, as the open segments were not uploaded yet; the comparison covers the offsets present). After `local.retention.ms` was set to 30000 and the local segments were deleted, the replay of the same topic gave 20002 pairs, 580 duplicates, 578 missing and 18841 offsets with a different record; the table was complete (20000 rows, 0 duplicates, 0 missing).
- **verified**: it depends on how the data was produced, not on the segment size alone. `stage2-replay-source-1m-unthrottled.txt` (default 1 MiB segments, `make batch N=20000` unthrottled): the tiered replay is complete, 20000 pairs, 0 duplicates, 0 missing, 0 different records. `stage2-replay-source-1m-paced.txt` (default segments, `PACE=2000`): tiered replay 20000 pairs, 527 duplicates, 527 missing, 19044 different records. In both 1 MiB runs the local-replay step has no result (the table did not exist yet, because all data was still in open segments; the script printed `no result`), so the local baseline comes from the 256 KiB run only. **Correction (Task 13b)**: this does not generalise to unthrottled batches. In `stage2-replay-kafka-tools-unthrottled.txt` (also `make batch N=20000`, unthrottled) partition 0's segment held 7 producer batches and its tiered replay was wrong (1109 repeated and 1109 missing pairs in partition 0, 3242 in all). What matters is how many producer batches a segment holds and how large they are, see the next section; **inference**: the unthrottled run above had one producer batch per partition (its transcript does not show the batch count).
- **inference**: the paced runs produce many small producer batches (about 40 records at 2000 events/s with `linger.ms=20`) and the unthrottled run few large ones (`stage2-batch-fields.txt` in `02-stage2-iceberg.md` shows about 1000 records per batch there); together with the earlier finding that the last record of a batch carries a different `batch_byte_offset`, this points at the plugin rebuilding Kafka batches from Parquet rows wrongly when there are many batches. I did not measure the batch sizes in these runs and did not test the reconstruction directly, so this is **not tested**. **Superseded (Task 13b)**: the reconstruction was then tested directly; the next section replaces this inference.

What the replay tells about the leader question: the history is served identically by the broker that wrote it and by one that did not (A against B, three runs), **and** wrongly by both once the stream has many small batches. So a leader change does not lose or duplicate history in the table, but I cannot say it keeps it intact for consumers, because the consumer path is already wrong without any leader change.

### Independent check with Kafka's own tools (Task 13b)

The replay numbers above came only from the lab's client (`lab/pairs.py`, `lab/replay.py`, both librdkafka). They were rechecked without it by `scripts/verify/stage2-replay-kafka-tools.sh`, on a fresh stack each time, with the default topic (1 MiB segments, `local.retention.ms=30000`) and `order_id` keys (unique per record, so a key names one record). Three transcripts: `stage2-replay-kafka-tools-paced.txt` (`make batch N=20000 PACE=2000`), `-unthrottled.txt` (`N=20000`, unthrottled) and `-one-record-batches.txt` (`N=200 PACE=20`: records 50 ms apart with `linger.ms=20`, so after the first one every producer batch holds one record). The ground truth is partition 0's original segment, dumped with `kafka-dump-log.sh --print-data-log` (and its `.index` file) on the leader before it was uploaded and deleted.

**Verified, the table is right** (all three transcripts, partition 0): every row's key and `kafka.batch_base_offset` equal the original segment's record and batch base offset at the same offset (6569 of 6569, 6569 of 6569, 66 of 66 rows). `kafka.batch_byte_offset` is mostly not the position of the record's own batch (equal on 245 of 6569, 666 of 6569 and 10 of 66 rows); on every row (6569, 6569, 66) it equals what a lookup in the segment's offset index returns for the record's offset, the position of the index entry with the largest offset not above it. This replaces the reading "lags one batch" in `02-stage2-iceberg.md`: that is what the index lookup gives when every batch is indexed, as with large unthrottled batches.

**Verified, the local log is served right**: Kafka's `kafka-console-consumer.sh` on partition 0 while the segment was still on disk delivered every offset once, with the original segment's key (6569, 6569 and 66 records; 0 differences).

**Verified, the broker itself serves the tiered history wrongly**, seen with Kafka's own consumer and with raw Fetch requests (no client library: `scripts/verify/rawfetch.py` sends Fetch v4 over a TCP socket and decodes the RecordBatch v2 format):

| Partition 0, history from tiered storage | Paced | Unthrottled | One-record batches |
|---|---|---|---|
| Producer batches in the original segment | 602 | 7 | 59 |
| Batches the leader returns for a Fetch at offset 0 (one response) | 117 | 7 | 3 |
| Returned batches whose offsets differ from the table's | 116 (shift up to 17) | 6 (shift up to 1079) | 0 |
| Last offset the returned batches reach (end offset) | 6557 (6569) | 5733 (6569) | 65 (66) |
| `kafka-console-consumer.sh` from offset 0: records delivered (of 6569, 6569, 66) | 6387 | 5460 | 66 |
| ... offsets missing between the first and last delivered | 171 | 274 | 0 |
| ... records whose key differs from the table's at that offset | 6246 | 3846 | 0 |
| `lab.pairs` on the same data: repeated / missing pairs, all partitions | 536 / 536 | 3242 / 3242 | 0 / 0 |

- **verified**: each returned batch holds records with contiguous true offsets in the true order (`offsetDelta` equal to the record index and a valid CRC32C on the four batches per partition the transcript prints), and a `baseOffset` equal to the table's `batch_base_offset` of the batch's first record (117 of 117, 7 of 7, 3 of 3); each returned batch is exactly one run of rows sharing a `batch_byte_offset` value (same counts). Against the original segment, every returned batch after the first starts at the last record of a producer batch (116 of 116, 6 of 6; 2 of 2 in the one-record run, where that record is the whole producer batch), and in the paced and unthrottled runs no returned batch equals a producer batch. So (arithmetic from these two facts) every record of such a batch is labelled lower than its true offset by the size of that producer batch minus one: 0 when producer batches hold one record, up to 17 in the paced run, up to 1079 unthrottled. Partitions 1 and 2 show the same in each transcript (paced: 119 of 120 and 116 of 117 returned batches shifted; unthrottled: 4 of 5 and 6 of 7; one-record: 0).
- **verified**: the labels overlap (paced, partition 0: 49 returned batches start at or below the previous batch's last offset) and leave gaps (53). Kafka's Java consumer never delivers the records labelled at or below its position (182 of partition 0 in the paced run, 1109 unthrottled) and delivers the rest under wrong offsets; librdkafka delivers every record, with repeated and missing offsets. `lab.pairs`' repeated and missing counts per partition equal the repeated and unused labels in the raw Fetch responses (paced: 182, 176, 178; unthrottled: 1109, 1087, 1046; one-record: 0), so the lab's tool reported what the broker sent.
- **verified, the stall**: a Fetch at the offset after the last returned label (6558 paced, 5734 unthrottled) returns error `NONE`, 0 bytes and no batch, three times 20 s apart and again after 300 more records were produced (high watermark 6657; those new records are served from offset 6569). A consumer that reaches that offset gets nothing more from the partition: the stall is the broker's answer, not a client timeout. The one-record run has no stall.
- **verified** (`## d)` sections): the leader logs no WARN, ERROR or exception during these reads, only `kafka.log.remote.RemoteLogReader` DEBUG lines (670 remote reads in the paced run, 2144 unthrottled, most of them consumers fetching again at the stall offset); no `io.aiven` logger line and no "Time taken" line appear.
- **inference**: the plugin rebuilds one Kafka batch per run of equal `batch_byte_offset` and takes its base offset from the run's first row. Those runs start where the offset index has an entry, and Kafka keys an index entry by the last offset of a batch, so the base offset taken belongs to the start of that producer batch, before the run. This is read from the plugin's input (the table) and output (the Fetch responses), which match on every batch above; the plugin's code was not read.
- **not tested**: compressed or transactional batches, other plugin versions, Fetch versions other than 4, whether a consumer stuck at the stall offset recovers once newer segments are tiered.

**Verdict: real, not an artefact of the lab's tools.** The table matches the original log, but in Iceberg mode the broker returns tiered history under wrong offsets whenever a rebuilt batch starts inside a producer batch of more than one record (paced and unthrottled alike in these runs), and stops short of the end offset; with one-record producer batches it was exact.

### Key to partition after a leader change

**Verified** (`## key mapping` sections in all three transcripts): with `LAB_KEY_FIELD=customer_id` (500 distinct customers, key equal to `value.customer_id` in every row: `rows_where_key_differs_from_customer_id` 0), `SELECT value.customer_id AS customer_id, count(DISTINCT kafka.partition) AS partitions FROM {table} GROUP BY 1 HAVING count(DISTINCT kafka.partition) > 1` returned no row before the restart (partial table), after the restart (A) and after the stop of the new leader (B), in each run (row counts 103000, 97962, 103000). A query on a top-level `customer_id` would fail: the column is `value.customer_id`. **inference**: the guarantee is the producer's key hash over a fixed partition count, which a leader change does not touch. **not tested**: a change of the partition count.

### What this means for an ingestion service

- A service that reads the Iceberg table can rely on exactly-once rows through a leader restart in the three runs (0 duplicates, 0 missing against the brokers' end offsets), but it must expect orphan Parquet files in the warehouse after an interrupted upload (1 file of 3418 or 3420 rows per hit run), so it needs a cleanup of files the table does not reference and must not count bucket files as data.
- A service that replays from the Kafka topic in Iceberg mode cannot trust what it gets back once the history is in tiered storage: offsets are mislabelled for every producer batch of more than one record that starts a rebuilt batch (527 to 3224 pairs repeated or missing in the runs above), Kafka's Java consumer silently skips some records (182 of 6569 in partition 0 of the paced Task 13b run) and every consumer stops short of the end offset. It should read history from the table, key its de-duplication on a field of the record such as `order_id`, and treat the topic replay as broken in this plugin version (Task 13b section above).

## Not tested (summary)

- Restart or kill at moments other than "just after `Copying` started"; a leader that stays down; the catalog or MinIO restarting during an upload; unthrottled load during the restart.
- The plugin code that rebuilds batches (the mechanism in the Task 13b section is read from its input and output only); whether newer plugin versions do the same; compressed or transactional batches; whether a consumer stuck at the stall offset recovers once newer segments are tiered.
- `segment.ms` below 5000, other `local.retention.ms` values in the idle case, other broker check intervals, higher rates and many partitions for the freshness numbers.
- The cause of the 5038 failed deliveries in the SIGKILL run.
