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
