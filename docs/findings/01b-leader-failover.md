# Experiment 4: losing the leader of a tiered partition

Numbers and log lines below are copied from `docs/findings/evidence/failover.txt`. Statements about configuration cite `compose.yml` and say so. Anything that is neither in the evidence file nor in `compose.yml` is labelled as inference.

## What was run

`scripts/exp-failover.sh` (fresh `make clean && make up`, topic `events`: 3 partitions, replication factor 3, `min.insync.replicas=2`, `segment.bytes=1048576`, `local.retention.ms=30000`):

1. Produce 60000 events at a target of 1000 events/s in the background (`make batch N=60000 PACE=1000`).
2. After 15 s, read the leader of partition 0 from `make topic-describe` and stop that broker (`make broker-stop`). It was broker 2.
3. After 20 s more, describe the topic again (asking a surviving broker), then wait for the producer to finish.
4. Restart the stopped broker and wait until every partition has a full ISR.
5. `make replay` from offset 0 and compare the record count with what the producer had acknowledged.

Follow-up, run by hand after the script (also in the evidence file): a second batch (`make batch N=20000 START=60000`) to see whether offload keeps working under the new leader, then another `make replay`.

## Leadership before, during and after

Before the stop:

```
Topic: events	Partition: 0	Leader: 2	Replicas: 2,3,1	Isr: 2,3,1
Topic: events	Partition: 1	Leader: 3	Replicas: 3,1,2	Isr: 3,1,2
Topic: events	Partition: 2	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3
```

After stopping broker 2 (20 s later), partition 0 has a new leader, broker 3, and the ISR has shrunk to the two live replicas on every partition that had broker 2 as a replica (all three):

```
Topic: events	Partition: 0	Leader: 3	Replicas: 2,3,1	Isr: 3,1
Topic: events	Partition: 1	Leader: 3	Replicas: 3,1,2	Isr: 3,1
Topic: events	Partition: 2	Leader: 1	Replicas: 1,2,3	Isr: 1,3
```

After restarting broker 2, it rejoined every ISR. Partition 0's leader stayed on broker 3; leadership did not move back (no preferred-leader election was triggered):

```
Topic: events	Partition: 0	Leader: 3	Replicas: 2,3,1	Isr: 3,1,2
Topic: events	Partition: 1	Leader: 3	Replicas: 3,1,2	Isr: 3,1,2
Topic: events	Partition: 2	Leader: 1	Replicas: 1,2,3	Isr: 1,3,2
```

Broker 3's log shows the handover about half a second (0.55 s, 17:49:48,637 to 17:49:49,188) after broker 2 received SIGTERM:

```
[2026-10-15 17:49:48,637] INFO Terminating process due to signal SIGTERM (org.apache.kafka.common.utils.LoggingSignalHandler)     (broker 2)
[2026-10-15 17:49:49,188] INFO Received leadership notifications with leader partitions [AvbO9pSzQXGn4d7umOyqjg:events-0, AvbO9pSzQXGn4d7umOyqjg:events-1] and follower partitions [AvbO9pSzQXGn4d7umOyqjg:events-2] ...   (broker 3)
```

## Producer behaviour

The producer's own log shows one broker disconnect, from broker 2 when it stopped: `localhost:9192/2: Disconnected: connection closed by peer`. The evidence does not show a reconnect, but the producer finished with:

```
INFO __main__: produced=60000 failed=0
```

So all 60000 events were acknowledged and none was reported as failed. (The other `FAIL ... Connection refused` lines in the evidence are against `[::1]` addresses on `localhost` ports; inference: the client tries the IPv6 address first and then falls back to IPv4, so they are unrelated to the failover. The evidence does not show the fallback itself.) Because `min.insync.replicas=2` and two replicas stayed alive, writes remained possible throughout.

## Replay result

```
[lab] produced=60000 replayed=60000
[lab] PASS: every acknowledged event was replayed exactly once after losing the leader of partition 0
```

The `PASS` wording above is what the script printed in this run. Its claim of "exactly once" was stronger than the check supports, so the script's message was afterwards reworded to say that only counts were compared (the experiment was not re-run).

The check compares only two counts: the producer's acknowledged count and the number of records `make replay` read from offset 0. They are equal. Equal counts are consistent with no loss and no duplicates, but they do not prove it: one lost record plus one duplicate would cancel out, and the check does not compare individual records (for example by sequence number). Offsets 0 to the high watermark were read contiguously for each partition (`first_offset` 0, `consumed` equal to `high_watermark`), which makes a gap in the offset range unlikely but says nothing about record content.

## Did offload resume under the new leader

Yes, but note what this run does and does not show.

- Broker 2 (stopped at 17:49:48) never logged a `Copying` or `Copied` line in this run (the `kafka-2` section of the evidence is empty), and the first `Copying` line for `events-0` is broker 3's at 17:50:29. Every segment of partition 0 in the bucket was therefore uploaded by broker 3, the new leader. Inference: the stop came about 15 s into the production, when partition 0 held well under one 1 MiB segment, so there was nothing rolled to upload yet. This run does **not** test whether a new leader keeps serving segments the old leader had already uploaded; it tests that the new leader takes over uploading.
- Segment 0 of partition 0 was copied by broker 3 about four seconds after partition 1's first segment was copied by its leader (broker 3 as well), which is plausibly the copy task running every 5 s (`KAFKA_REMOTE_LOG_MANAGER_TASK_INTERVAL_MS: 5000` in `compose.yml`; the interval itself is not visible in the evidence):

```
[2026-10-15 17:50:29,192] INFO [RemoteLogManager=3 partition=AvbO9pSzQXGn4d7umOyqjg:events-0] Copying 00000000000000000000.log to remote storage. (kafka.log.remote.RemoteLogManager$RLMCopyTask)
[2026-10-15 17:50:29,339] INFO [RemoteLogManager=3 partition=AvbO9pSzQXGn4d7umOyqjg:events-0] Copied 00000000000000000000.log to remote storage with segment-id: RemoteLogSegmentId{topicIdPartition=AvbO9pSzQXGn4d7umOyqjg:events-0, id=YLuEEqo1Qr6FIZXUfVfJ4w} (kafka.log.remote.RemoteLogManager$RLMCopyTask)
```

- Follow-up batch (`make batch N=20000 START=60000`, started 17:51:28Z, `produced=20000 failed=0`). The objects for partition 0 went from two segments to three. The new segment (base offset 20118) was copied by broker 3, the new leader, at 17:52:04:

```
[2026-10-15 17:52:04,499] INFO [RemoteLogManager=3 partition=AvbO9pSzQXGn4d7umOyqjg:events-0] Copying 00000000000000020118.log to remote storage. (kafka.log.remote.RemoteLogManager$RLMCopyTask)
[2026-10-15 17:52:04,609] INFO [RemoteLogManager=3 partition=AvbO9pSzQXGn4d7umOyqjg:events-0] Copied 00000000000000020118.log to remote storage with segment-id: RemoteLogSegmentId{topicIdPartition=AvbO9pSzQXGn4d7umOyqjg:events-0, id=iWdC0u7CTMKOcETw-su7JQ} (kafka.log.remote.RemoteLogManager$RLMCopyTask)
```

```
[2026-10-15 17:52:04 UTC]   105B STANDARD events-AvbO9pSzQXGn4d7umOyqjg/0/00000000000000020118-iWdC0u7CTMKOcETw-su7JQ.indexes
[2026-10-15 17:52:04 UTC] 453KiB STANDARD events-AvbO9pSzQXGn4d7umOyqjg/0/00000000000000020118-iWdC0u7CTMKOcETw-su7JQ.log
[2026-10-15 17:52:04 UTC]   736B STANDARD events-AvbO9pSzQXGn4d7umOyqjg/0/00000000000000020118-iWdC0u7CTMKOcETw-su7JQ.rsm-manifest
```

The bucket ended at 27 objects (9 per partition: three segments, each with `.log`, `.indexes` and `.rsm-manifest`), 5.9 MiB. No other broker logged a `Copying`/`Copied` line for `events-0` at any point.

A caveat on timing: the uploaded segments of the second round are smaller than the 1 MiB `segment.bytes` (534 KiB for base offset 13231, 453 KiB for base offset 20118), so they were not rolled by size. The base-offset-13231 segment of partition 0 was copied at 17:51:24, before the follow-up batch started at 17:51:28Z, and the next one (base offset 20118, which holds the follow-up batch's records) at 17:52:04. All three partitions uploaded their second and third segments within seconds of each other (17:51:24 to 17:51:25 in the `ls-minio` timestamps; the 17:51:26 is the `Copied` line of broker 3 for `events-1` in the broker log; then 17:52:04 to 17:52:06 in the `ls-minio` timestamps). The roll events themselves are not in this evidence file. A later, separate check on a fresh stack (`docs/findings/evidence/stage1-segment-roll.txt`, discussed in `01-stage1-offload.md`) found that an idle active segment is closed on every replica about `local.retention.ms` (30 s here) after the last write. That fits the two steps about 40 s apart seen here, each following the end of a producer burst (the first producer finished at 17:50:51, the follow-up batch started at 17:51:28Z); it is an inference for this run, which was not repeated with roll logging. Consequently the 13231 segment was not triggered by the follow-up batch, only the 20118 one (and 19963 / 19919 on partitions 1 and 2) was.

## What the new leader knew

After the follow-up batch I ran `make replay` again: `consumed=80000` (partition 0: 26701, partition 1: 26747, partition 2: 26552, each from `first_offset` 0 to the high watermark), equal to 60000 + 20000 events produced.

During that replay broker 3 (the new leader of partition 0) served partition 0's history from the bucket. Its `RemoteLogReader` logged three read pairs for `events-0` (the mapping of reads to segments is not observable in the logs), and neither broker 1 nor broker 2 logged any for `events-0`:

```
[2026-10-15 17:53:47,541] DEBUG Reading records from remote storage for topic partition events-0 (kafka.log.remote.RemoteLogReader)
[2026-10-15 17:53:47,547] DEBUG Finished reading records from remote storage for topic partition events-0 (kafka.log.remote.RemoteLogReader)
[2026-10-15 17:53:47,572] DEBUG Reading records from remote storage for topic partition events-0 (kafka.log.remote.RemoteLogReader)
[2026-10-15 17:53:47,582] DEBUG Finished reading records from remote storage for topic partition events-0 (kafka.log.remote.RemoteLogReader)
[2026-10-15 17:53:47,585] DEBUG Reading records from remote storage for topic partition events-0 (kafka.log.remote.RemoteLogReader)
[2026-10-15 17:53:47,596] DEBUG Finished reading records from remote storage for topic partition events-0 (kafka.log.remote.RemoteLogReader)
```

Which segments each of those reads covered is not in the log lines (the plugin's own DEBUG logging is not visible, because its slf4j has no provider here; see the SLF4J "No providers found" lines in `docs/findings/evidence/stage1-replay-window.txt`), so any mapping of the three read pairs to the three uploaded segments would be inference from counts. What the evidence supports: broker 3, which became leader after the original leader had gone, performed remote reads for partition 0 during the replay, and the replay returned 80000 records. By configuration, the remote log metadata topic has replication factor 3 (`KAFKA_RLMM_CONFIG_REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR: 3` in `compose.yml`), so every broker can see which segments exist; the evidence itself does not show that metadata being read. The limitation above still applies: the segments read had all been uploaded by broker 3 itself, so this run does not show the case of a new leader reading objects that the previous leader wrote.

## Limits of this experiment

- One run, one stop of one broker, a stop by `docker compose stop`, which the evidence shows as a SIGTERM to broker 2 (a controlled shutdown is what `docker compose stop` is expected to trigger; it was not captured in the evidence). A hard kill or a network partition was not tested.
- The old leader had not uploaded anything yet, so the "new leader continues from the old leader's uploads" case was not exercised. Doing that would need the stop to happen after at least one segment of the partition has been copied (for example by stopping later or lowering `segment.bytes`).
- The producer is not configured as idempotent, so a duplicate-free, loss-free result is at most an observation of this run (replayed count equal to acknowledged count), not a guarantee and not a per-record check.
