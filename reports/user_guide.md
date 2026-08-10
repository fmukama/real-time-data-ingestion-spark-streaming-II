# User Guide

How to run this pipeline from nothing to rows in PostgreSQL. Every command here is a `make` target ; you should
never need to type a `docker` command directly.


## 1. Prerequisites

| Requirement | Notes |
|---|---|
| **Docker Desktop** | Must be **running** before any command below. On Windows, ensure the WSL2 backend is enabled |
| **`make`** | Via Git Bash, WSL, or `choco install make`. If you have none of these, every target's underlying command is listed in the [README](../README.md#make-targets) |
| **~4 GB free RAM** | Spark's JVM plus PostgreSQL |
| **Ports 5432, 8888, 4040, 8080 free** | If a native PostgreSQL already owns 5432, change `POSTGRES_HOST_PORT` in `.env` (step 2). 8080 is Adminer ; change `ADMINER_HOST_PORT` if something else owns it |

Nothing else. No local Python, no local Spark, no local PostgreSQL ; everything runs in containers.


## 2. First-time setup

```bash
git clone <repository-url>
```

```bash
cd Real-Time-Data-Ingestion-II
```

Create your environment file from the tracked template:

```bash
cp .env.example .env
```

> `.env` is gitignored; `.env.example` is tracked. Both containers read `.env`, so there is exactly one source of
> truth for credentials and tuning knobs. The defaults work as-is ; you only need to edit it if port 5432 is
> already taken.

Build the Spark image (base image + JDBC driver + Python requirements). **This takes several minutes the first
time** and is cached afterwards:

```bash
make build
```

Start all three containers:

```bash
make up
```

`make up` waits for PostgreSQL's healthcheck to pass before starting Spark, so Spark never connects into the
window where the database is up but not yet accepting connections.

**The database schema is created automatically.** `sql/postgres_setup.sql` is mounted into the postgres image's
init directory, which runs every `.sql` file there once, when the data directory is first initialised. There is
no separate "run the setup script" step.

Confirm the containers are healthy:

```bash
docker compose ps
```

You should see `postgres`, `spark` and `adminer`, all `running`.


## 3. Run the pipeline

You need **two terminals**, because both processes run in the foreground.

### Terminal 1 ; start the streaming job

```bash
make stream
```

Leave this running. It prints one block per micro-batch:

```
[sinks]: batch 3: events -> 1368 in, 1368 inserted, 0 skipped (already present)
[sinks]: batch 3: metrics -> 10 window/category row(s) upserted
[sinks]: batch 3: 32 event(s) quarantined
```

Nothing will appear until data arrives ; that is correct. The query polls every 10 seconds by default and an
empty poll is silent.

### Terminal 2 ; generate events

```bash
make generate
```

That runs at the defaults: **50 events/sec, forever, with 2% deliberately malformed rows.** To control it, pass
`ARGS`:

```bash
make generate ARGS="--rate 200 --duration 120"
```

| Flag | Default | Meaning |
|---|---|---|
| `--rate` | 50 | target events per second |
| `--batch-size` | 100 | events per CSV file |
| `--duration` | *(forever)* | stop after this many seconds |
| `--bad-rate` | 0.02 | fraction of deliberately malformed events |
| `--seed` | *(random)* | seed for reproducible output |

The malformed fraction is intentional ; it is what demonstrates the quarantine path. Setting `--bad-rate 0`
produces a clean stream but exercises less of the pipeline.


## 4. Watch it work

**The narrative log** ; what the pipeline is doing, in order:

```bash
tail -f logs/pipeline.log
```

**The Spark UI** ; live query progress, batch durations, input rate: <http://localhost:4040>

**The files moving through the three-directory dance:**

```bash
ls data/staging data/incoming data/archive
```

`incoming/` should stay small. Files land there and are archived once consumed.

> **Expect files to linger in `incoming/` after you stop the generator.** Spark's `cleanSource` housekeeping only
> runs as part of a batch that finds **new** files ; an idle poll never archives anything. Those files have
> already been committed to the database; only the tidy-up is pending. Nothing is lost.


## 5. Verify the data

Run the full verification suite ; row counts, quarantine breakdown, latency percentiles, throughput, and the
`event_metrics` cross-check:

```bash
make metrics
```

Or open a SQL prompt and look yourself:

```bash
make psql
```

### Prefer a browser? `make adminer`

```bash
make adminer
```

Adminer is a browser-based SQL client for the same database ; click through tables, sort and filter columns, and
run ad hoc SQL without a terminal or a desktop client installed. The target starts the container if it isn't
already up, then prints a link:

```
http://localhost:8080/?pgsql=postgres&username=streaming&db=ecommerce
```

Those URL parameters do real work: they preselect **PostgreSQL** in the *System* dropdown and fill in the server,
username and database. **You type only the password** ; the `POSTGRES_PASSWORD` value from your `.env`. Adminer
deliberately never accepts a password from a URL or an environment variable, which is the right call: a URL
lands in shell history, terminal scrollback and the browser address bar.

If you navigate to `http://localhost:8080` directly instead of using the printed link, two fields need attention:

| Field | Value | Why |
|---|---|---|
| **System** | `PostgreSQL` | Defaults to MySQL/MariaDB, and cannot be changed by configuration ; only the URL parameter preselects it |
| **Server** | `postgres` | **Not** `localhost` ; inside Docker, `localhost` is the Adminer container itself. Prefilled for you either way |
| **Username** / **Database** | `streaming` / `ecommerce` | Whatever `POSTGRES_USER` / `POSTGRES_DB` are set to in `.env` |

Once in, the three tables are `events`, `events_quarantine` and `event_metrics`.

> Adminer is a convenience, not part of the pipeline. It stores nothing, and nothing depends on it ; stopping or
> removing the container has no effect on ingestion.

```sql
SELECT count(*) FROM events;
```

```sql
SELECT rejection_reason, count(*) FROM events_quarantine GROUP BY 1 ORDER BY 2 DESC;
```

```sql
SELECT * FROM event_metrics ORDER BY window_start DESC, category LIMIT 10;
```

**The check that proves the aggregate is correct** ; does each stored window match what the raw table says for
that same minute?

```sql
SELECT em.window_start, em.category,
       em.total_revenue, raw.raw_total_revenue,
       em.event_count,   raw.raw_event_count
FROM event_metrics em
JOIN (SELECT date_trunc('minute', event_time) AS window_start, category,
             coalesce(sum(revenue),0) AS raw_total_revenue, count(*) AS raw_event_count
      FROM events GROUP BY 1,2) raw
  ON raw.window_start = em.window_start AND raw.category = em.category
ORDER BY em.window_start DESC;
```

Every row should match exactly.

Connecting from a desktop client (DBeaver, pgAdmin, TablePlus) instead? See
[`postgres_connection_details.txt`](postgres_connection_details.txt) ; note the host differs depending on whether
you connect from inside Docker or from Windows.

---

## 6. Stop

**Ctrl+C** in each terminal. The streaming job handles it and shuts down cleanly.

Then stop the containers, keeping all data:

```bash
make down
```


## 7. Housekeeping ; and the one you will actually need

### `make clean` ; delete generated data and logs, **keep the database**

```bash
make clean
```

Safe to run mid-project. Clears `data/staging`, `data/incoming`, `data/archive` and `logs/`.

### `make reset` ; `clean` **plus** drop the database and checkpoint volumes

```bash
make reset
```

**You will need this more often than you expect, and it is not optional housekeeping.** Two things in this stack
are one-shot:

1. **`sql/postgres_setup.sql` only runs when the pgdata volume is empty.** Edit the schema and nothing happens
   until the volume is dropped. If you change the DDL and the database seems to ignore you, this is why.
2. **A Spark checkpoint is bound to its query.** Spark refuses to restart against a checkpoint written by a
   different schema or a different query plan, and fails with an error about an incompatible or unresolved plan.

`make reset` is destructive by design ; it drops the database ; but every byte it removes is regenerable.

**If the streaming job fails on startup with a checkpoint error, `make reset` is the fix.**


## 8. Running the tests

Fast unit tests, no database required (seconds):

```bash
make test
```

Full suite including integration tests against the real PostgreSQL (requires `make up` first):

```bash
make verify
```

The manual test plan, with real recorded results, is in [`test_cases.md`](test_cases.md).


## 9. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `Cannot connect to the Docker daemon` | Docker Desktop isn't running | Start Docker Desktop, wait for it to report Running |
| `port is already allocated` on 5432 | A native PostgreSQL owns the port | Change `POSTGRES_HOST_PORT` in `.env`, then `make down && make up` |
| `port is already allocated` on 8080 | Something else owns 8080 (it is a popular default) | Change `ADMINER_HOST_PORT` in `.env`, then `make down && make up` |
| Adminer says the connection failed | *System* left on MySQL/MariaDB, or *Server* set to `localhost` | Use the link `make adminer` prints ; it sets both correctly |
| Streaming job fails immediately with a checkpoint/plan error | Stale checkpoint from an earlier query shape | `make reset` |
| Schema changes to `postgres_setup.sql` have no effect | Init script only runs on an empty data directory | `make reset` |
| `make stream` prints nothing | No data yet, or the generator isn't running | Start `make generate` in a second terminal |
| Files piling up in `data/incoming/` while the job runs | Consumer is behind the producer | Lower `--rate`, or raise `MAX_FILES_PER_TRIGGER` in `.env` |
| A few files remain in `incoming/` after stopping | Expected ; `cleanSource` only runs on a batch with new files | Nothing to fix; the data is already in Postgres |
| `make: command not found` | No `make` on PATH | Use Git Bash/WSL, or run the underlying commands from the README table |
