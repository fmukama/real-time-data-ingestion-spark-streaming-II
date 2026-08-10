SPARK := spark
PG    := postgres

# ARGS lets you pass flags through without editing the Makefile, e.g.
#   make generate ARGS="--rate 1000 --duration 300"
ARGS ?=

.PHONY: help build up down logs shell psql generate stream metrics test verify clean reset

help:
	@echo "Environment"
	@echo "  make build     - build the spark image (base + JDBC driver + requirements)"
	@echo "  make up        - start postgres + spark (waits for postgres to be healthy)"
	@echo "  make down      - stop both containers, keep volumes"
	@echo "  make logs      - follow container logs (the JupyterLab token appears here)"
	@echo "  make shell     - bash inside the spark container"
	@echo "  make psql      - psql inside the postgres container"
	@echo ""
	@echo "Pipeline"
	@echo "  make generate  - run the event generator      ARGS=\"--rate 100 ...\""
	@echo "  make stream    - run the streaming job (events + windowed metrics)"
	@echo "  make metrics   - run sql/verification_queries.sql and print results"
	@echo ""
	@echo "Quality"
	@echo "  make test      - unit tests only (no docker-postgres needed)"
	@echo "  make verify    - full suite including integration tests"
	@echo ""
	@echo "Housekeeping"
	@echo "  make clean     - delete generated data + logs, KEEP the database"
	@echo "  make reset     - clean + DROP the database and checkpoint volumes"

# --- Environment ---

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

shell:
	docker compose exec $(SPARK) bash

psql:
	docker compose exec $(PG) sh -c 'psql -U $$POSTGRES_USER -d $$POSTGRES_DB'

# --- Pipeline ---

generate:
	docker compose exec $(SPARK) python data_generator.py $(ARGS)

stream:
	docker compose exec $(SPARK) python spark_streaming_to_postgres.py $(ARGS)

metrics:
	docker compose exec -T $(PG) sh -c 'psql -U $$POSTGRES_USER -d $$POSTGRES_DB' < sql/verification_queries.sql

# --- Quality ---

test:
	docker compose exec $(SPARK) python -m pytest tests/ -m "not integration"

verify:
	docker compose exec $(SPARK) python -m pytest tests/

# --- Housekeeping ---

# Keeps the database. Safe to run mid-project.
#
# archive/ is NOT a flat *.csv glob like staging/incoming -- confirmed directly:
# Spark's cleanSource=archive mirrors each consumed
# file's full absolute source path under archive_dir (e.g.
# archive/home/jovyan/work/data/incoming/events_....csv), not a flat copy. A
# flat glob here silently left that nested tree behind on every real run.
clean:
	rm -f data/staging/*.csv data/incoming/*.csv
	find data/archive -mindepth 1 ! -name '.gitkeep' -delete
	rm -f logs/*.log logs/*.jsonl

# `down -v` drops the pgdata AND checkpoints volumes. You WILL need this more
# than you expect, because two things in this stack are one-shot:
#   - sql/postgres_setup.sql runs only when pgdata is empty, so schema edits
#     don't apply until the volume is dropped;
#   - a checkpoint is bound to its query, so Spark refuses to restart against
#     one written by a different schema or a different stateful plan.
# Destructive by design: every byte it removes is regenerable.
reset: clean
	@echo ">>> Dropping pgdata + checkpoints volumes (database contents will be lost)"
	docker compose down -v
	docker compose up -d
	@echo ">>> Stack recreated. Schema re-applied from sql/ on first boot."
