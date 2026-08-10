# Real-Time Data Ingestion ; Spark Structured Streaming -> PostgreSQL

A real-time pipeline simulating an e-commerce platform's user-activity stream: a generator emits fake events as
CSV files, Spark Structured Streaming consumes them as they land, cleans and validates them, and writes them
idempotently into PostgreSQL.


## Quickstart

Requires Docker Desktop running, and `make` (Git Bash / WSL / Chocolatey).

```bash
cp .env.example .env
```

```bash
make build && make up
```

```bash
make help
```

`make up` starts both containers and waits for PostgreSQL's healthcheck before starting Spark ; so Spark never
connects into the window where the database is up but not yet accepting connections.

## Verified environment

Every version below was checked against its registry before pinning, not assumed.

| Component | Version | Note |
|---|---|---|
| Base image | `quay.io/jupyter/pyspark-notebook:2026-08-08` | dated tag, never `latest` |
| Spark | 4.2.0 | bundled in the image |
| Java / Python | 21 / 3.13.14 | bundled |
| PostgreSQL | `postgres:16-alpine` | see note below |
| JDBC driver | `postgresql-42.7.13.jar` | baked into the image at build time |

> **Why PostgreSQL 16 and not 18:** verified directly ; the `postgres:18` image moved `PGDATA` to
> `/var/lib/postgresql/18/docker`. The near-universal `pgdata:/var/lib/postgresql/data` volume mount therefore
> binds onto an unused empty directory on 18, silently losing the database on every container recreate. 16-alpine
> keeps the classic path, is supported to Nov 2028, and every feature this project uses (`ON CONFLICT`,
> `percentile_cont`, `TIMESTAMPTZ`) long predates it. If you move to 18+, mount `/var/lib/postgresql` instead.


## Make targets

| Target | Does |
|---|---|
| `make build` | build the spark image (base + JDBC driver + requirements) |
| `make up` | start postgres + spark, waiting for postgres to be healthy |
| `make down` | stop both containers, keep volumes |
| `make logs` | follow container logs (the JupyterLab token appears here) |
| `make shell` | bash inside the spark container |
| `make psql` | psql inside the postgres container |
| `make generate` | run the event generator ; `ARGS="--rate 1000 --duration 300"` |
| `make stream` | run the streaming job |
| `make metrics` | run `sql/verification_queries.sql` and print results |
| `make test` | unit tests only, no database needed |
| `make verify` | full suite including integration tests |
| `make clean` | delete generated data + logs, **keep** the database |
| `make reset` | `clean` **plus** drop the pgdata and checkpoint volumes |

> **`make reset` Needed:** Two things in this stack are one-shot:
> `sql/postgres_setup.sql` only runs when the pgdata volume is empty, so schema edits don't apply until it's
> dropped; and a Spark checkpoint is bound to its query, so Spark refuses to restart against one written by a
> different schema or stateful plan.

---

## Ports

| Port | Service | From Windows |
|---|---|---|
| 8888 | JupyterLab | `http://localhost:8888/lab` ; token from `make logs` |
| 4040 | Spark UI | `http://localhost:4040` ; the streaming query's live progress page |
| 5432 | PostgreSQL | `localhost:5432` |

> **The hostname that trips everyone up:** inside the Docker network the database host is `postgres` (the compose
> service name) ; inside a container, `localhost` is that container itself. From Windows it's `localhost`. Both
> are correct; they're different vantage points.

Checkpoints are deliberately absent from this tree ; they live in a named Docker volume, because Windows bind
mounts don't reliably honour the atomic-rename semantics Structured Streaming's checkpointing depends on.

## Deliverables map

All nine deliverables the brief names, and where each one lives:

| Brief asks for | Lives at | |
|---|---|---|
| `data_generator.py` | [`data_generator.py`](data_generator.py) 
| `spark_streaming_to_postgres.py` | [`spark_streaming_to_postgres.py`](spark_streaming_to_postgres.py) 
| `postgres_setup.sql` | [`sql/postgres_setup.sql`](sql/postgres_setup.sql) ; also the container's init script 
| `postgres_connection_details.txt` | [`reports/postgres_connection_details.txt`](reports/postgres_connection_details.txt) 
| `project_overview.md` | [`reports/project_overview.md`](reports/project_overview.md) 
| `user_guide.md` | [`reports/user_guide.md`](reports/user_guide.md) 
| `test_cases.md` | [`reports/test_cases.md`](reports/test_cases.md) ; 11 cases, all executed 
| `performance_metrics.md` | [`reports/performance_metrics.md`](reports/performance_metrics.md) 
| `system_architecture.png` | [`reports/system_architecture.png`](reports/system_architecture.png) 

Beyond the brief:

| Extra | Lives at |
|---|---|
| Decision records ; the contested calls, with what was found while building | [`reports/decisions/`](reports/decisions/) |
| Automated test suite ; 86 tests | [`tests/`](tests/) |
| Windowed aggregate (`event_metrics`) ; the stretch goal | [`src/transforms.py`](src/transforms.py), [`src/sinks.py`](src/sinks.py) |

