# Stage 1: plain tiered storage

Numbers and log lines below are copied from the files under `docs/findings/evidence/`. Statements about configuration cite `compose.yml` and say so. Anything neither in an evidence file nor in `compose.yml` is labelled as inference. The run described is the offload smoke test `tests/smoke/03-offload.sh` (60000 events, one `make batch`, then replay) plus a second replay and the captures listed below. The leader-failover experiment has its own file: [01b-leader-failover.md](01b-leader-failover.md).

## What was run

Commands: `make up`, `make topic-describe`, `make batch N=60000` (the topic itself is created by the `topic-init` service during `make up`; `make topic` would re-create it), `make ls-minio`, `make ls-local`, `make replay`. The smoke test `tests/smoke/03-offload.sh` does exactly this and waits for the uploads and for the local deletion.

Cluster: 3 brokers (Apache Kafka 4.0.0 in KRaft mode, each broker also a controller), MinIO, Karapace (`compose.yml`). Topic settings, from `stage1-topic-describe.txt`: 3 partitions, replication factor 3, `min.insync.replicas=2`, `segment.bytes=1048576`, `local.retention.ms=30000`, `retention.ms=-1`, `remote.storage.enable=true`. (`local.retention.ms` is 30 s, the `compose.yml` default.) The broker-side tiering knobs that matter for timing are in `compose.yml`: `KAFKA_REMOTE_LOG_MANAGER_TASK_INTERVAL_MS: 5000` and `KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS: 5000`.

## How partitions spread over brokers

From `docs/findings/evidence/stage1-topic-describe.txt`:

```
Topic: events	TopicId: JPJMm7XzScWTiF45EfLS6g	PartitionCount: 3	ReplicationFactor: 3	Configs: remote.storage.enable=true,min.insync.replicas=2,local.retention.ms=30000,segment.bytes=1048576,retention.ms=-1
	Topic: events	Partition: 0	Leader: 2	Replicas: 2,3,1	Isr: 2,3,1	Elr: 	LastKnownElr: 
	Topic: events	Partition: 1	Leader: 3	Replicas: 3,1,2	Isr: 3,1,2	Elr: 	LastKnownElr: 
	Topic: events	Partition: 2	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3	Elr: 	LastKnownElr: 
```

Each partition has one leader and two followers, and the leaders are on three different brokers: partition 0 is led by broker 2, partition 1 by broker 3, partition 2 by broker 1. The preferred replica (first in `Replicas`) is the leader for all three, and all three replicas are in sync. Producers write only to the leader; the followers copy from it.

## What landed in MinIO

From `docs/findings/evidence/stage1-ls-minio.txt` (`make ls-minio`, bucket `kafka-tiered`):

```
[2026-10-15 17:43:22 UTC]    30B STANDARD events-JPJMm7XzScWTiF45EfLS6g/0/00000000000000000000-4nsFJpe5STmh5PGM_Hwllw.indexes
[2026-10-15 17:43:22 UTC] 696KiB STANDARD events-JPJMm7XzScWTiF45EfLS6g/0/00000000000000000000-4nsFJpe5STmh5PGM_Hwllw.log
[2026-10-15 17:43:22 UTC]   724B STANDARD events-JPJMm7XzScWTiF45EfLS6g/0/00000000000000000000-4nsFJpe5STmh5PGM_Hwllw.rsm-manifest
[2026-10-15 17:43:52 UTC]    78B STANDARD events-JPJMm7XzScWTiF45EfLS6g/0/00000000000000010000-w-9lO0pbRpesReScP-mwdg.indexes
[2026-10-15 17:43:52 UTC] 699KiB STANDARD events-JPJMm7XzScWTiF45EfLS6g/0/00000000000000010000-w-9lO0pbRpesReScP-mwdg.log
[2026-10-15 17:43:52 UTC]   735B STANDARD events-JPJMm7XzScWTiF45EfLS6g/0/00000000000000010000-w-9lO0pbRpesReScP-mwdg.rsm-manifest
[2026-10-15 17:43:22 UTC]   118B STANDARD events-JPJMm7XzScWTiF45EfLS6g/1/00000000000000000000-gxzsnnpSQDy8MGr9LQpHsQ.indexes
[2026-10-15 17:43:22 UTC]1015KiB STANDARD events-JPJMm7XzScWTiF45EfLS6g/1/00000000000000000000-gxzsnnpSQDy8MGr9LQpHsQ.log
[2026-10-15 17:43:22 UTC]   731B STANDARD events-JPJMm7XzScWTiF45EfLS6g/1/00000000000000000000-gxzsnnpSQDy8MGr9LQpHsQ.rsm-manifest
[2026-10-15 17:43:52 UTC]    78B STANDARD events-JPJMm7XzScWTiF45EfLS6g/1/00000000000000014688-4_hOVokWSLWhaxjWQcjOcA.indexes
[2026-10-15 17:43:52 UTC] 363KiB STANDARD events-JPJMm7XzScWTiF45EfLS6g/1/00000000000000014688-4_hOVokWSLWhaxjWQcjOcA.log
[2026-10-15 17:43:52 UTC]   735B STANDARD events-JPJMm7XzScWTiF45EfLS6g/1/00000000000000014688-4_hOVokWSLWhaxjWQcjOcA.rsm-manifest
[2026-10-15 17:43:22 UTC]   654B STANDARD events-JPJMm7XzScWTiF45EfLS6g/2/00000000000000000000-uPNwK786S9qqvh_FL0Lsng.indexes
[2026-10-15 17:43:22 UTC]1002KiB STANDARD events-JPJMm7XzScWTiF45EfLS6g/2/00000000000000000000-uPNwK786S9qqvh_FL0Lsng.log
[2026-10-15 17:43:22 UTC]   734B STANDARD events-JPJMm7XzScWTiF45EfLS6g/2/00000000000000000000-uPNwK786S9qqvh_FL0Lsng.rsm-manifest
[2026-10-15 17:43:52 UTC]    86B STANDARD events-JPJMm7XzScWTiF45EfLS6g/2/00000000000000014549-HI4OnvyQRlK9eaB8fQoFHQ.indexes
[2026-10-15 17:43:52 UTC] 370KiB STANDARD events-JPJMm7XzScWTiF45EfLS6g/2/00000000000000014549-HI4OnvyQRlK9eaB8fQoFHQ.log
[2026-10-15 17:43:52 UTC]   735B STANDARD events-JPJMm7XzScWTiF45EfLS6g/2/00000000000000014549-HI4OnvyQRlK9eaB8fQoFHQ.rsm-manifest
4.1MiB	18 objects	kafka-tiered
```

Every uploaded segment is three objects: the segment data (`.log`), the segment's offset/time/transaction indexes bundled in one object (`.indexes`), and the plugin's manifest (`.rsm-manifest`, the metadata the plugin needs to read the segment back). The object name starts with the segment's base offset, followed by the id of the remote log segment. Each partition has its own prefix, `events-<topicId>/<partition>/`; here `events-JPJMm7XzScWTiF45EfLS6g/0/`, `/1/` and `/2/`. There are two segments per partition (18 objects, 4.1 MiB). The sizes are well below the 1 MiB `segment.bytes` for some segments (696 KiB, 699 KiB, 363 KiB, 370 KiB); see Surprises.

Which broker uploaded which partition, from `docs/findings/evidence/stage1-uploads.txt` (the `Copied` lines of `RemoteLogManager`, per broker):

```
# Upload lines, all brokers: docker compose logs --no-log-prefix kafka-N | grep -E 'RLMCopyTask\)$' | grep -E ' Copied ' (raw)
== kafka-1
[2026-10-15 17:43:22,686] INFO [RemoteLogManager=1 partition=JPJMm7XzScWTiF45EfLS6g:events-2] Copied 00000000000000000000.log to remote storage with segment-id: RemoteLogSegmentId{topicIdPartition=JPJMm7XzScWTiF45EfLS6g:events-2, id=uPNwK786S9qqvh_FL0Lsng} (kafka.log.remote.RemoteLogManager$RLMCopyTask)
[2026-10-15 17:43:52,783] INFO [RemoteLogManager=1 partition=JPJMm7XzScWTiF45EfLS6g:events-2] Copied 00000000000000014549.log to remote storage with segment-id: RemoteLogSegmentId{topicIdPartition=JPJMm7XzScWTiF45EfLS6g:events-2, id=HI4OnvyQRlK9eaB8fQoFHQ} (kafka.log.remote.RemoteLogManager$RLMCopyTask)
== kafka-2
[2026-10-15 17:43:22,691] INFO [RemoteLogManager=2 partition=JPJMm7XzScWTiF45EfLS6g:events-0] Copied 00000000000000000000.log to remote storage with segment-id: RemoteLogSegmentId{topicIdPartition=JPJMm7XzScWTiF45EfLS6g:events-0, id=4nsFJpe5STmh5PGM_Hwllw} (kafka.log.remote.RemoteLogManager$RLMCopyTask)
[2026-10-15 17:43:52,741] INFO [RemoteLogManager=2 partition=JPJMm7XzScWTiF45EfLS6g:events-0] Copied 00000000000000010000.log to remote storage with segment-id: RemoteLogSegmentId{topicIdPartition=JPJMm7XzScWTiF45EfLS6g:events-0, id=w-9lO0pbRpesReScP-mwdg} (kafka.log.remote.RemoteLogManager$RLMCopyTask)
== kafka-3
[2026-10-15 17:43:22,685] INFO [RemoteLogManager=3 partition=JPJMm7XzScWTiF45EfLS6g:events-1] Copied 00000000000000000000.log to remote storage with segment-id: RemoteLogSegmentId{topicIdPartition=JPJMm7XzScWTiF45EfLS6g:events-1, id=gxzsnnpSQDy8MGr9LQpHsQ} (kafka.log.remote.RemoteLogManager$RLMCopyTask)
[2026-10-15 17:43:52,833] INFO [RemoteLogManager=3 partition=JPJMm7XzScWTiF45EfLS6g:events-1] Copied 00000000000000014688.log to remote storage with segment-id: RemoteLogSegmentId{topicIdPartition=JPJMm7XzScWTiF45EfLS6g:events-1, id=4_hOVokWSLWhaxjWQcjOcA} (kafka.log.remote.RemoteLogManager$RLMCopyTask)
```

Each broker uploaded only the partition it leads: kafka-2 uploaded partition 0, kafka-3 partition 1, kafka-1 partition 2, matching the leaders in the topic description above. Followers never uploaded anything; the same segment is not copied three times. The files `stage1-broker-N-log.txt` hold the raw broker log around the second upload (17:43:52) and show the same single `Copied` line per broker.

## What stayed on the broker

From `docs/findings/evidence/stage1-ls-local.txt` (`make ls-local`, after the replays):

```
== kafka-1
-rw-r--r-- 1 appuser appuser 0 Oct 15 17:43 /var/lib/kafka/data/events-0/00000000000000020118.log
-rw-r--r-- 1 appuser appuser 0 Oct 15 17:43 /var/lib/kafka/data/events-1/00000000000000019963.log
-rw-r--r-- 1 appuser appuser 0 Oct 15 17:43 /var/lib/kafka/data/events-2/00000000000000019919.log
== kafka-2
-rw-r--r-- 1 appuser appuser 0 Oct 15 17:43 /var/lib/kafka/data/events-0/00000000000000020118.log
-rw-r--r-- 1 appuser appuser 0 Oct 15 17:43 /var/lib/kafka/data/events-1/00000000000000019963.log
-rw-r--r-- 1 appuser appuser 0 Oct 15 17:43 /var/lib/kafka/data/events-2/00000000000000019919.log
== kafka-3
-rw-r--r-- 1 appuser appuser 0 Oct 15 17:43 /var/lib/kafka/data/events-0/00000000000000020118.log
-rw-r--r-- 1 appuser appuser 0 Oct 15 17:43 /var/lib/kafka/data/events-1/00000000000000019963.log
-rw-r--r-- 1 appuser appuser 0 Oct 15 17:43 /var/lib/kafka/data/events-2/00000000000000019919.log
```

Every broker, leaders and followers alike, holds exactly one `.log` file per partition, and it is empty (0 bytes): the active segment, whose base offset (20118, 19963, 19919) is the partition's high watermark. The three base offsets add up to 60000, the number of events produced. All older segments were deleted locally. `docs/findings/evidence/stage1-local-and-cache.txt` counts the `.log` files directly and gets 1 per partition per broker.

Leaders and followers delete their own copies by the same rule. The raw broker output around the replay (`docs/findings/evidence/stage1-replay-window.txt`) shows the six follower copies of the second segments being deleted, all within 70 ms of each other, for example:

```
[2026-10-15 17:44:00,955] INFO [UnifiedLog partition=events-0, dir=/var/lib/kafka/data] Deleting segment LogSegment(baseOffset=10000, size=715940, lastModifiedTime=1792086200659, largestRecordTimestamp=1792086200619) due to local log retention time 30000ms breach based on the largest record timestamp in the segment (kafka.log.UnifiedLog)
```

The wording is the broker's own: the deletion is "due to local log retention time 30000ms breach based on the largest record timestamp in the segment". The leaders' deletions of the same segments are before the captured window, so the log does not show them, but the empty directory listing above shows that they happened.

The brokers also keep a small cache of index files for remote segments, in `/var/lib/kafka/data/remote-log-index-cache`. After the replays (`stage1-local-and-cache.txt`):

```
# /var/lib/kafka/data/remote-log-index-cache on each broker (ls -l --time-style=+%H:%M:%S, UTC)
== kafka-1
total 16
-rw-r--r-- 1 264 17:44:02 0_uPNwK786S9qqvh_FL0Lsng.index
-rw-r--r-- 1 372 17:44:02 0_uPNwK786S9qqvh_FL0Lsng.timeindex
-rw-r--r-- 1   0 17:44:02 0_uPNwK786S9qqvh_FL0Lsng.txnindex
-rw-r--r-- 1  32 17:44:02 14549_HI4OnvyQRlK9eaB8fQoFHQ.index
-rw-r--r-- 1  36 17:44:02 14549_HI4OnvyQRlK9eaB8fQoFHQ.timeindex
-rw-r--r-- 1   0 17:44:02 14549_HI4OnvyQRlK9eaB8fQoFHQ.txnindex
== kafka-2
total 12
-rw-r--r-- 1  0 17:44:02 0_4nsFJpe5STmh5PGM_Hwllw.index
-rw-r--r-- 1 12 17:44:02 0_4nsFJpe5STmh5PGM_Hwllw.timeindex
-rw-r--r-- 1  0 17:44:02 0_4nsFJpe5STmh5PGM_Hwllw.txnindex
-rw-r--r-- 1 24 17:44:02 10000_w-9lO0pbRpesReScP-mwdg.index
-rw-r--r-- 1 36 17:44:02 10000_w-9lO0pbRpesReScP-mwdg.timeindex
-rw-r--r-- 1  0 17:44:02 10000_w-9lO0pbRpesReScP-mwdg.txnindex
== kafka-3
total 16
-rw-r--r-- 1 40 17:44:02 0_gxzsnnpSQDy8MGr9LQpHsQ.index
-rw-r--r-- 1 60 17:44:02 0_gxzsnnpSQDy8MGr9LQpHsQ.timeindex
-rw-r--r-- 1  0 17:44:02 0_gxzsnnpSQDy8MGr9LQpHsQ.txnindex
-rw-r--r-- 1 24 17:44:02 14688_4_hOVokWSLWhaxjWQcjOcA.index
-rw-r--r-- 1 36 17:44:02 14688_4_hOVokWSLWhaxjWQcjOcA.timeindex
-rw-r--r-- 1  0 17:44:02 14688_4_hOVokWSLWhaxjWQcjOcA.txnindex
```

Each broker's cache holds entries only for the partition it leads (kafka-1: partition 2, kafka-2: partition 0, kafka-3: partition 1), which is consistent with reads going through the leader.

## Replay from offset 0

`make replay` prints one JSON line on stdout with `consumed` and, per partition, `first_offset`, `last_offset`, `high_watermark`. The stage 1 JSON line itself was not saved as evidence; the header of `stage1-replay-window.txt` records that the client logged `replayed 60000 records in 0.52 s` (19:44:02 local, 17:44:02 UTC) for the replay in the smoke test, which equals the 60000 events produced. The smoke test also asserts, for every partition, `first_offset == 0` and `consumed == high_watermark`. For the format, this is the JSON line of a later replay in the failover experiment (a different run, 80000 events, from `docs/findings/evidence/failover.txt`):

```
{"consumed": 80000, "partitions": {"0": {"consumed": 26701, "first_offset": 0, "last_offset": 26700, "high_watermark": 26701}, "1": {"consumed": 26747, "first_offset": 0, "last_offset": 26746, "high_watermark": 26747}, "2": {"consumed": 26552, "first_offset": 0, "last_offset": 26551, "high_watermark": 26552}}, "seconds": 0.62}
```

For the stage 1 run, the high watermarks follow from the local listing above: 20118, 19963 and 19919 records in partitions 0, 1 and 2 (derived, not printed by the replay in this evidence).

What served the replay: at the time of the replay each broker's only local segment per partition was the empty active one, so every record came from MinIO. The broker log with `kafka.log.remote=DEBUG` (set in `compose.yml` through `KAFKA_LOG4J_LOGGERS`) shows `RemoteLogReader` serving the reads on each partition's leader only. First read of each broker in the first replay window (`stage1-replay-window.txt`, window A):

```
kafka-1: [2026-10-15 17:44:02,017] DEBUG Reading records from remote storage for topic partition events-2 (kafka.log.remote.RemoteLogReader)
kafka-1: [2026-10-15 17:44:02,116] DEBUG Reading records from remote storage for topic partition events-2 (kafka.log.remote.RemoteLogReader)
kafka-2: [2026-10-15 17:44:02,018] DEBUG Reading records from remote storage for topic partition events-0 (kafka.log.remote.RemoteLogReader)
kafka-2: [2026-10-15 17:44:02,119] DEBUG Reading records from remote storage for topic partition events-0 (kafka.log.remote.RemoteLogReader)
kafka-3: [2026-10-15 17:44:02,019] DEBUG Reading records from remote storage for topic partition events-1 (kafka.log.remote.RemoteLogReader)
kafka-3: [2026-10-15 17:44:02,118] DEBUG Reading records from remote storage for topic partition events-1 (kafka.log.remote.RemoteLogReader)
```

Each of those is followed by a `Finished reading records from remote storage` line, and each leader logs two such pairs per partition, one per uploaded segment (inference: the number of pairs matches the number of segments; the log lines do not name the segment). The second replay (window B, 17:44:13) shows the same pattern. What the logs do not show: the lookup of the segment in the remote log metadata is not visible on the read path, and neither are the plugin's own `io.aiven` lines (see Surprises). The line `Time taken to collect: 0 aborted transactions for RemoteLogSegmentMetadata{...}` that sits between the two lines in the broker log is written by `RemoteLogManager`, not by `RemoteLogReader`; it does name the remote segment (id, start and end offset, broker id), which is the only place in this evidence where the read is tied to a particular segment.

## Surprises

**Karapace startup race (fixed in commit 853e80e).** Karapace answered `GET /subjects` as soon as its HTTP server was up, but a fresh Karapace is "not master" for about 5 s after its election, and POSTs sent in that window are forwarded to itself and hang. In addition, Karapace's first group-coordinator lookup created `__consumer_offsets` (50 partitions, replication factor 3), and the coordinator moved from node 3 to node 1 while the partitions elected leaders. The fix is in `compose.yml`: the Karapace health check also requires `master_available` to be true, and the `topic-init` service warms up `__consumer_offsets` and waits until all 50 partitions have a leader and a full ISR before Karapace starts. POSTs, such as registering a schema, hang in that window; that is what the health check now keeps clients away from. (Source for the mechanism: the comments in `compose.yml`, which were written when the race was diagnosed; the commit message itself only names the fix.)

**The plugin's own logging is invisible (NOP slf4j).** `compose.yml` sets `io.aiven.kafka.tieredstorage=DEBUG`, but no `io.aiven` line ever appears in the broker logs. The broker starts with these lines (`stage1-replay-window.txt`, last section, from `docker compose logs kafka-1 | grep SLF4J`):

```
SLF4J(W): No SLF4J providers were found.
SLF4J(W): Defaulting to no-operation (NOP) logger implementation
SLF4J(W): See https://www.slf4j.org/codes.html#noProviders for further details.
SLF4J(W): Class path contains SLF4J bindings targeting slf4j-api versions 1.7.x or earlier.
SLF4J(W): Ignoring binding found at [jar:file:/opt/kafka/libs/log4j-slf4j-impl-2.24.3.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J(W): See https://www.slf4j.org/codes.html#ignoredBindings for an explanation.
```

The plugin uses slf4j 2, which looks for a provider and finds none: the only binding on the class path, `log4j-slf4j-impl-2.24.3.jar`, targets slf4j 1.x and is ignored. So the plugin logs to a no-operation (NOP) logger. Consequence for this lab: every plugin-internal question (how the segment is found in the manifest, which chunks are fetched, cache hits) cannot be answered from the logs; only Kafka's own `kafka.log.remote` and `org.apache.kafka.server.log.remote` loggers are visible. Cause and effect are read from the startup lines; the fix (adding an slf4j 2 provider to the plugin class path) was not attempted.

**Segments roll earlier than `segment.bytes`.** The lab sets `segment.bytes=1048576`, but several segments are well under 1 MiB: 696 KiB, 699 KiB, 363 KiB and 370 KiB in this run, and 534 KiB, 453 KiB, 467 KiB and 457 KiB in the failover run. In the failover run all three partitions rolled their active segment together, about 40 s apart in two steps. A bounded investigation (about 10 minutes of experiments, evidence in `docs/findings/evidence/stage1-segment-roll.txt`) found that this is not a time-based roll setting: `segment.ms` is 604800000 (7 days), `segment.jitter.ms` is 0, `log.roll.ms` and `log.roll.jitter.ms` are unset (`kafka-configs.sh --describe --all` for the topic and broker 1). What the experiment did find is that an idle active segment is closed, on every replica, about `local.retention.ms` after the last write:

| `local.retention.ms` | last write acknowledged | `Rolled new log segment` on kafka-1, 2, 3 | delay |
|---|---|---|---|
| 30000 | 19:15:04Z | 19:15:38,892 / 19:15:36,944 / 19:15:38,897 | 32 to 34 s |
| 10000 | 19:16:52Z | 19:17:03,892 / 19:17:06,944 / 19:17:03,897 | 11 to 14 s |

(All three partitions rolled on all three brokers in each case; the rows list the `events-2` line of each broker, the other partitions roll within 2 ms.) The roll time follows the topic's `local.retention.ms` plus the 5 s retention check interval, not the segment size. That explains the failover figures: the producer finished at 17:50:51 and partition 0's second segment was uploaded at 17:51:24 (33 s later), and after the follow-up batch started at 17:51:28 the next segments were uploaded at 17:52:04 to 17:52:06; those two roll-and-upload steps are the "about 40 s apart". It also explains the second segment in the stage 1 run (uploaded 17:43:52, 30 s after the first uploads). Inference, not shown by the logs (the `Rolled new log segment` line does not state a reason): Kafka rolls the active segment instead of leaving it when all its data is past the local retention, so that the segment can be tiered and deleted locally; this matches the behaviour of `local.retention.ms` in tiered topics. The first segments of the stage 1 run (696 KiB for partition 0, 1015 KiB and 1002 KiB for the others) are not covered by this: they were closed during the burst of 60000 events from `make batch`, before any idle period. Untested inference: a roll happens when the next producer batch would not fit, and the unthrottled batch producer sends large batches (partition 0's first segment holds exactly 10000 records, offsets 0 to 9999, which is librdkafka's default maximum batch size in messages); the batch sizes were not inspected. So "segments close only at `segment.bytes`" is not true here, and at a low event rate a lab user will see tiering only about half a minute after production stops (or after the segment fills, which takes many minutes at the default 50 events/s of `make produce`).

**SIGPIPE under `pipefail` in the first smoke scripts.** The smoke scripts run with `set -euo pipefail` (`scripts/lib.sh`). A check such as `unzip -l jar | grep -q pattern` or `docker compose logs kafka-N | grep -q pattern` makes `grep -q` exit on the first match and close the pipe; the writer then dies with SIGPIPE (exit 141), and `pipefail` turns that into a failure of a check that actually succeeded. It depends on timing and output size, so it fails intermittently. The scripts committed in 3063cc3 already contain the fix: capture the output first and grep the variable (`listing="$(unzip -l ...)"`, `broker_log="$(docker compose logs ...)"`, then `grep -q ... <<<"$listing"`); the comments in `tests/smoke/00-plugin.sh` and `tests/smoke/02-stack.sh` name the problem. The git history only contains the fixed form, so the failing version is described from those comments, not from a recorded failure. Small checks such as `echo "$describe" | grep -q ...` in `02-stack.sh` still pipe, but they write a few hundred bytes, far below a pipe buffer, so the writer has finished before `grep` exits.
