# Real-Time Data Ingestion ; Spark Structured Streaming -> PostgreSQL

A generator emits fake e-commerce user-activity events as CSV files. Spark Structured Streaming consumes them as
they land, validates them, and writes them idempotently into PostgreSQL ; rejected rows are quarantined with a
reason rather than dropped, and a per-minute windowed aggregate is maintained in the same transaction.

Full walkthrough: [`reports/user_guide.md`](reports/user_guide.md) ; design rationale:
[`reports/project_overview.md`](reports/project_overview.md).


## Setup

Requires Docker Desktop **running**, and `make` (Git Bash / WSL / Chocolatey).

```bash
cp .env.example .env
make build && make up      # first build takes several minutes, cached after
```

`make up` waits for PostgreSQL's healthcheck before starting Spark, so Spark never connects into the window where
the database is up but not yet accepting connections. The schema applies itself on first boot. `make help` lists
every target.


## Run it ; two terminals

Both processes run in the foreground, so each needs its own terminal.

**Terminal 1 ; the streaming job.** Start this first, so it is watching before the first file lands:

```bash
make stream
```

**Terminal 2 ; the generator.** Defaults to 50 events/sec, forever, 2% deliberately malformed rows. Tune it with
`ARGS` (`--rate`, `--batch-size`, `--duration`, `--bad-rate`, `--seed`):

```bash
make generate                                        # or: ARGS="--rate 200 --duration 120"
```

Terminal 1 then prints one block per micro-batch:

```
[sinks]: batch 3: events -> 491 in, 491 inserted, 0 skipped (already present)
[sinks]: batch 3: metrics -> 10 window/category row(s) upserted
[sinks]: batch 3: 9 event(s) quarantined
```

**Stop:** `Ctrl+C` in each terminal, then `make down`.

> **Stopping the generator does not stop the stream.** Spark drains the files already on disk, writes them, then
> sits idle polling for more ; a streaming query runs until you stop it. Files lingering in `data/incoming/`
> afterwards are already committed ; `cleanSource` only archives on a batch that finds **new** files. Nothing is lost.

Then check the results:

```bash
make metrics      # row counts, quarantine reasons, p50/p95/max latency, throughput, aggregate cross-check
make adminer      # same data in a browser; prints a URL with everything but the password filled in
```


## Make targets

| Target | Does |
|---|---|
| `make build` | build the spark image (base + JDBC driver + requirements) |
| `make up` | start postgres + spark + adminer, waiting for postgres to be healthy |
| `make down` | stop all three containers, keep volumes |
| `make logs` | follow container logs (the JupyterLab token appears here) |
| `make shell` | bash inside the spark container |
| `make psql` | psql inside the postgres container |
| `make adminer` | browse the database in a browser ; prints a prefilled login URL |
| `make generate` | run the event generator ; `ARGS="--rate 1000 --duration 300"` |
| `make stream` | run the streaming job |
| `make metrics` | run `sql/verification_queries.sql` and print results |
| `make test` | unit tests only, no database needed |
| `make verify` | full suite including integration tests |
| `make clean` | delete generated data + logs, **keep** the database |
| `make reset` | `clean` **plus** drop the pgdata and checkpoint volumes |


## Ports

| Port | Service | From Windows |
|---|---|---|
| 8888 | JupyterLab | `http://localhost:8888/lab` ; token from `make logs` |
| 4040 | Spark UI | `http://localhost:4040` ; the streaming query's live progress page |
| 8080 | Adminer | `http://localhost:8080` ; use `make adminer` for a prefilled link |
| 5432 | PostgreSQL | `localhost:5432` |








## Deliverables map

| Brief asks for | Lives at |
|---|---|
| `data_generator.py` | [`data_generator.py`](data_generator.py) |
| `spark_streaming_to_postgres.py` | [`spark_streaming_to_postgres.py`](spark_streaming_to_postgres.py) |
| `postgres_setup.sql` | [`sql/postgres_setup.sql`](sql/postgres_setup.sql) ; also the container's init script |
| `postgres_connection_details.txt` | [`reports/postgres_connection_details.txt`](reports/postgres_connection_details.txt) |
| `project_overview.md` | [`reports/project_overview.md`](reports/project_overview.md) |
| `user_guide.md` | [`reports/user_guide.md`](reports/user_guide.md) |
| `test_cases.md` | [`reports/test_cases.md`](reports/test_cases.md) ; 11 cases, all executed |
| `performance_metrics.md` | [`reports/performance_metrics.md`](reports/performance_metrics.md) |
| `system_architecture.png` | [`reports/system_architecture.png`](reports/system_architecture.png) |

