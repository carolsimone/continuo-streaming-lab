# STAGE=iceberg switches every compose call to include the Iceberg override.
ifeq ($(STAGE),iceberg)
export COMPOSE_FILE := compose.yml:compose.iceberg.yml
# Iceberg mode reads the record key as Avro too, so the producer must send Avro keys. A value from
# the command line (make LAB_KEY_SERDE=string ...) or the shell wins over this default.
LAB_KEY_SERDE ?= avro
endif
# Producer key knobs (see `python -m lab.produce --help`). Empty = the producer's own default.
export LAB_KEY_SERDE LAB_KEY_FIELD

PLUGIN_VERSION := 1.1.1
PLUGIN_URL     := https://github.com/Aiven-Open/tiered-storage-for-apache-kafka/releases/download/v$(PLUGIN_VERSION)
CORE_SHA256    := 053480aa4ea5d3b667df824b4c4425f469243df3c89fa8c50edbf9ec88300e4f
S3_SHA256      := 3e0ea6ce17033c1fcf74e0f6f292411cb3e2afcc1798b51125bb15e5b7500017

COMPOSE := docker compose

# Port overrides from a gitignored .env (KEY=value lines) reach the host tools too, not just compose.
# As in compose, a value already set in the shell wins over .env. A variable that neither sets is
# exported empty, which compose (`${VAR:-default}`) and lab/endpoints.py both treat as unset.
PORT_VARS := KAFKA_1_PORT KAFKA_2_PORT KAFKA_3_PORT SCHEMA_REGISTRY_PORT MINIO_API_PORT MINIO_CONSOLE_PORT ICEBERG_REST_PORT
$(foreach v,$(PORT_VARS),$(if $($(v)),$(eval _shell_$(v) := $($(v)))))
-include .env
$(foreach v,$(PORT_VARS),$(if $(_shell_$(v)),$(eval $(v) := $(_shell_$(v)))))
export $(PORT_VARS)

# Producer knobs: `make produce COUNT=10000 RATE=200` (COUNT=0 runs until `make produce-stop`).
COUNT ?= 0
RATE  ?= 50

# Broker to stop/start: `make broker-stop BROKER=2`.
BROKER ?=

KAFKA_TOPICS = /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-$(or $(VIA),1):19092
MC := $(COMPOSE) run --rm --no-deps -T --entrypoint sh minio-init -c

.DEFAULT_GOAL := help
.PHONY: help plugin up down rebuild clean clean-plugin ps logs produce produce-stop batch \
        topic topic-describe ls-minio ls-local replay query broker-stop broker-start test

help: ## List targets
	@grep -hE '^[a-z-]+:.*## ' $(firstword $(MAKEFILE_LIST)) | awk -F':.*## ' '{printf "  %-14s %s\n", $$1, $$2}'

plugin: plugin/.installed ## Download the Aiven plugin into ./plugin (checksum-verified)

plugin/.installed:
	@set -e; tmp=$$(mktemp -d); trap 'rm -rf "$$tmp"' EXIT; \
	for pair in core:$(CORE_SHA256) s3:$(S3_SHA256); do \
	  name=$${pair%%:*}; sha=$${pair##*:}; \
	  echo "downloading $$name-$(PLUGIN_VERSION).tgz"; \
	  curl -fsSL -o "$$tmp/$$name.tgz" "$(PLUGIN_URL)/$$name-$(PLUGIN_VERSION).tgz"; \
	  echo "$$sha  $$tmp/$$name.tgz" | shasum -a 256 -c -; \
	done; \
	rm -rf plugin; mkdir -p plugin/core plugin/s3; \
	tar -xzf "$$tmp/core.tgz" -C plugin/core --strip-components=1; \
	tar -xzf "$$tmp/s3.tgz" -C plugin/s3 --strip-components=1; \
	touch plugin/.installed

up: plugin ## Start 3 brokers, MinIO, Karapace and create buckets + topic (idempotent)
	$(COMPOSE) up -d --wait

down: ## Stop containers; keep Kafka and MinIO data
	$(COMPOSE) --profile producer down --remove-orphans

rebuild: plugin ## Rebuild from scratch: wipe containers and data, rebuild images, start again
	$(COMPOSE) --profile producer down --volumes --remove-orphans
	$(COMPOSE) --profile producer build
	$(COMPOSE) up -d --wait --force-recreate

clean: ## Remove containers and all data (volumes)
	$(COMPOSE) --profile producer down --volumes --remove-orphans

clean-plugin: ## Delete the downloaded plugin
	rm -rf plugin

ps: ## Show container status
	$(COMPOSE) --profile producer ps -a

logs: ## Follow logs of all running containers
	$(COMPOSE) --profile producer logs -f --tail=50

produce: up ## Start the producer in the background (COUNT, RATE)
	EVENTS_COUNT=$(COUNT) EVENTS_RATE=$(RATE) $(COMPOSE) --profile producer up -d --build --force-recreate --no-deps producer

produce-stop: ## Stop the producer (graceful flush)
	$(COMPOSE) --profile producer stop producer
	@$(COMPOSE) --profile producer logs --no-log-prefix --tail=2 producer

batch: ## Produce a finite batch from the host (N=20000, PACE=0, SCHEMA=v1, START=0)
	uv run python -m lab.produce --count $(or $(N),20000) --rate $(or $(PACE),0) \
	  --schema-version $(or $(SCHEMA),v1) --start-seq $(or $(START),0)

topic: ## (Re)create the tiered events topic (PARTITIONS, REPLICATION_FACTOR, SEGMENT_BYTES, SEGMENT_MS, LOCAL_RETENTION_MS)
	scripts/create-topic.sh

topic-describe: ## Leader, replicas and in-sync replicas per partition (VIA=n asks broker n, default 1)
	$(COMPOSE) exec kafka-$(or $(VIA),1) $(KAFKA_TOPICS) --describe --topic events

ls-minio: ## List tiered objects in MinIO (per partition)
	@$(MC) 'mc alias set l http://minio:9000 minioadmin minioadmin >/dev/null && mc ls -r l/kafka-tiered; mc du l/kafka-tiered'

ls-local: ## List the segments each broker still holds on its own disk
	@for n in 1 2 3; do \
	  echo "== kafka-$$n"; \
	  $(COMPOSE) exec -T kafka-$$n sh -c 'ls -l /var/lib/kafka/data/events-*/*.log 2>/dev/null' || echo "  (down or empty)"; \
	done

replay: ## Replay events from offset 0 (served from MinIO once local segments are gone)
	uv run python -m lab.replay

query: ## Query the Iceberg table via DuckDB (SQL="... FROM {table}")
	uv run python -m lab.query $(if $(SQL),--sql "$(SQL)")

broker-stop: ## Stop one broker: make broker-stop BROKER=2
	@test -n "$(BROKER)" || { echo "usage: make broker-stop BROKER=1|2|3" >&2; exit 2; }
	$(COMPOSE) stop kafka-$(BROKER)

broker-start: ## Start a stopped broker: make broker-start BROKER=2
	@test -n "$(BROKER)" || { echo "usage: make broker-start BROKER=1|2|3" >&2; exit 2; }
	$(COMPOSE) start kafka-$(BROKER)

test: ## Run Python unit tests
	uv run pytest -q
