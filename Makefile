SPARK := spark
PG    := postgres
ADM   := adminer

# ARGS lets you pass flags through without editing the Makefile, e.g.
#   make generate ARGS="--rate 1000 --duration 300"
ARGS ?=

.PHONY: help build up down logs shell psql adminer generate stream metrics test verify clean reset

help:
	@echo "Environment"
	@echo "  make build     - build the spark image (base + JDBC driver + requirements)"
	@echo "  make up        - start postgres + spark (waits for postgres to be healthy)"
	@echo "  make down      - stop both containers, keep volumes"
	@echo "  make logs      - follow container logs (the JupyterLab token appears here)"
	@echo "  make shell     - bash inside the spark container"
	@echo "  make psql      - psql inside the postgres container"
	@echo "  make adminer   - browse the database in a browser (prints a prefilled URL)"
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

# Same database as `make psql`, in a browser instead of a terminal. Starts the
# container if it isn't already up, so this works standalone without `make up`.
#
# The URL carries `?pgsql=postgres&username=...&db=...`, which preselects
# PostgreSQL in the "System" dropdown and fills in server, username and
# database. Only the password is left to type -- Adminer never prefills a
# password from the URL or the environment, by design, and that is the right
# call: a URL ends up in shell history, the terminal scrollback and the
# browser's address bar.
#
# The three values are read from .env rather than hardcoded, so this target
# cannot drift from the credentials the containers actually use.
#
# Read with grep+cut, deliberately NOT by sourcing .env. `. ./.env` looks
# tidier and breaks immediately on this project's own file: TRIGGER_INTERVAL is
# `10 seconds`, unquoted, so a shell sourcing it would try to run `seconds` as
# a command. tr -d '\r' guards the other direction -- .env is LF today, but
# editing it in Notepad would append CR to every value and silently corrupt
# the URL rather than fail.
adminer:
	docker compose up -d $(ADM)
	@port=$$(grep -E '^ADMINER_HOST_PORT=' .env 2>/dev/null | cut -d= -f2- | tr -d '\r'); \
	 user=$$(grep -E '^POSTGRES_USER=' .env 2>/dev/null | cut -d= -f2- | tr -d '\r'); \
	 db=$$(grep -E '^POSTGRES_DB=' .env 2>/dev/null | cut -d= -f2- | tr -d '\r'); \
	 echo ""; \
	 echo ">>> Adminer is up. Open this (server/username/database prefilled, PostgreSQL preselected):"; \
	 echo ""; \
	 echo "      http://localhost:$${port:-8080}/?pgsql=postgres&username=$${user:-streaming}&db=$${db:-ecommerce}"; \
	 echo ""; \
	 echo ">>> Password: the POSTGRES_PASSWORD value in your .env (not printed here)."; \
	 echo ">>> Server is 'postgres', not 'localhost' -- inside Docker, localhost is Adminer itself."

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
