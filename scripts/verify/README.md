# Stage 2 verification scripts

Each script prints every command it runs (`$ ...`) and then the command's real output. The transcripts
committed in `docs/findings/evidence/stage2-*.txt` were produced by redirecting a script's stdout and stderr:
`scripts/verify/stage2-files.sh > docs/findings/evidence/stage2-files.txt 2>&1`.

Run order, from a clean checkout:

1. `tests/smoke/04-iceberg.sh` starts the Iceberg stack, produces 20000 events and waits for the table. Leave
   the stack running. Then, on that stack and in this order (these only read):
   `stage2-files.sh`, `stage2-parquet.sh`, `stage2-catalog.sh`, `stage2-raw-columns.sh`, `stage2-batch-fields.sh`.
2. `stage2-key-partition.sh` produces a second batch (customer_id keys) and waits for it; then
   `stage2-offsets.sh` (reads the table and the brokers, needs the data of step 2).
3. `stage2-persistence.sh` is destructive (it takes the containers down and ends with `make clean`): run it last.
4. `stage2-failure-modes.sh` is independent: it runs `make clean` and `make up STAGE=iceberg` itself, produces
   deliberately failing batches and ends with `make clean`.

The scripts need Docker, `uv`, `curl` and `python3` on the host, the same as the rest of the lab. `lib.sh`
holds the shared helpers and sets `STAGE=iceberg`.

Stage 2 experiments 6 and 7 (findings: `docs/findings/03-freshness-and-duplicates.md`):

5. `stage2-freshness.sh` is independent: for every case it runs `make clean`, `make up STAGE=iceberg`, recreates the topic
   with the settings under test and runs `python -m lab.freshness`. `FRESHNESS_REPS` (default 3) sets the repeats; a full
   run takes about 20 minutes.
6. `stage2-duplicates.sh` is independent and ends with `make clean`. `DUP_TRIGGER=copying|delay` and `DUP_KILL=0|1` choose
   when the leader of partition 0 is restarted and whether it is killed (see the header of the script). It uses
   `python -m lab.pairs`, which compares the table, a replay of the topic and the broker end offsets by
   `(partition, offset)`.
7. `stage2-replay-kafka-tools.sh` is independent and ends with `make clean` (about 6 minutes). It checks the replay result of
   experiment 7 without the lab's client: `kafka-dump-log.sh` on partition 0's original segment and offset index before upload,
   Kafka's `kafka-console-consumer.sh` on the local log and then on the tiered history, raw Fetch requests (`rawfetch.py`,
   standard library only) to each partition's leader, and `python -m lab.pairs` last for comparison. The helpers
   `cc_compare.py`, `dump_to_jsonl.py`, `rows_compare.py`, `index_vs_table.py` and `fetch_vs_table.py` compare these outputs
   (standard library only; see each file's docstring). `KT_PACE`, `KT_BATCH`, `KT_DEEP`, `KT_KEY_FIELD`, `KT_BATCH_NUM_MESSAGES` and
   `KT_FIRST_LOCAL_RETENTION_MS` choose the run; the header lists the five committed transcripts and their settings. Line 1 of
   `stage2-replay-kafka-tools-customer-keys-unthrottled.txt` reads `later 300003600000` (a header bug, fixed since): that run
   had `local.retention.ms=3600000` first and 30000 later.
