# Project Overview

A real-time ingestion pipeline for e-commerce user-activity events: a producer emits fake events as CSV files, a
Spark Structured Streaming job consumes them as they land, cleans and validates them, and writes them
**idempotently** into PostgreSQL ; alongside a continuously-maintained per-minute revenue aggregate.

**Diagram:** [`system_architecture.png`](system_architecture.png) (components and data flow) and
[`micro_batch_sequence.png`](micro_batch_sequence.png) (what happens inside one micro-batch, and why the pipeline
survives a crash).


## The system in one paragraph

`data_generator.py` writes batches of events to `data/staging/`, then **atomically renames** each finished file
into `data/incoming/`. Spark watches `incoming/` only, so it can never observe a half-written file. Each
micro-batch is parsed against an explicit schema, cast, normalised, enriched with derived columns, then **split**
into valid rows and rejected rows. Valid rows go to `events` through an idempotent upsert; rejected rows go to
`events_quarantine` with a reason; the batch's contribution to the per-minute aggregate goes to `event_metrics`
in the *same database transaction* as the events write. Spark then commits the batch to its checkpoint, which is
what makes a restart resume rather than re-do.

## Components

| Component | File | Responsibility |
|---|---|---|
| **Producer** | [`data_generator.py`](../data_generator.py) -> [`src/generator.py`](../src/generator.py) | Emits realistic events as CSV, including a controlled fraction of deliberately malformed rows |
| **Event contract** | [`src/schema.py`](../src/schema.py) | The single source of truth for the 9 event columns ; the generator's CSV header, Spark's read schema, and the SQL DDL all derive from it |
| **Streaming job** | [`spark_streaming_to_postgres.py`](../spark_streaming_to_postgres.py) -> [`src/streaming.py`](../src/streaming.py) | Builds the file source, assembles source -> transforms -> sinks, starts the query |
| **Transforms** | [`src/transforms.py`](../src/transforms.py) | Pure `DataFrame -> DataFrame`: cast, normalise, derive, classify, split. No I/O, so it is testable in milliseconds without Docker |
| **Sinks** | [`src/sinks.py`](../src/sinks.py) | The `foreachBatch` writers and the transaction that binds them |
| **Observability** | [`src/monitoring.py`](../src/monitoring.py) | A `StreamingQueryListener` capturing per-batch progress to `logs/metrics.jsonl` |
| **Config** | [`src/config.py`](../src/config.py) | Every path, credential and tuning knob, read from the environment with defaults |
| **Schema bootstrap** | [`sql/postgres_setup.sql`](../sql/postgres_setup.sql) | The DDL ; and simultaneously the container's init script, so there is no separate setup step |

Both entrypoint scripts at the repository root are **thin CLI wrappers**; every real line of logic lives in
`src/`. That is what lets the whole pipeline be unit-tested without starting a streaming query.


## Guarantees

| Property | Guarantee | Mechanism |
|---|---|---|
| **Source** | at-least-once | Spark's file source + checkpoint. A crash between writing and committing replays the batch |
| **`events`** | **effectively exactly-once** | `event_id` is **producer-generated**, so a replayed row carries the same id and is caught by `ON CONFLICT (event_id) DO NOTHING` |
| **`event_metrics`** | effectively exactly-once | Accumulates only the rows the events `INSERT … RETURNING` *actually inserted* ; a replay returns nothing, so it adds nothing |
| **`events` + `event_metrics` together** | atomic | Both writes share **one transaction**. They cannot diverge |
| **`events_quarantine`** | at-least-once (may duplicate) | **Deliberate.** A structurally corrupt line may have no usable `event_id` to key on. It is the diagnostic table, not the financial record |

> The idempotency guarantee depends entirely on `event_id` coming from the **producer**. If Spark generated it
> instead, replayed rows would get fresh ids, conflict with nothing, and duplicate perfectly.

**Windowing uses event time, not processing time.** An event delayed by the network still belongs to the minute
in which it actually occurred ; that is what makes the aggregate *correct* rather than merely *timely*.

## Performance

Measured, not estimated ; full report in [`performance_metrics.md`](performance_metrics.md). Headlines:

- The **Postgres write (`addBatch`) is 73–82% of every batch's time** ; the pipeline is sink-bound, as predicted.
- Baseline p50 latency **5.7s against a 10s trigger**, matching the theoretical half-interval prediction almost
  exactly.
- A 5× rate increase with no tuning change pushed p50 from **5.7s -> 53.9s**: the system falls behind under
  sustained overload rather than degrading gracefully.
- Quarantine rate held at **1.74–2.01%** across every scenario, confirming validation behaviour is a function of
  the data rather than of load.


<!-- 
## What would change in a real company

This project is deliberately honest about being a single-machine simulation. What a production deployment would
change, and what it would keep:

| Concern | Here | In production |
|---|---|---|
| **Transport** | CSV files in a watched directory | Kafka / Kinesis. Files are the assignment's premise, and the weakest link ; directory listing is O(files) and there is no replay beyond what's on disk. `build_source()` is the *only* function that knows the source is files, so this swap is genuinely scoped |
| **Credentials** | `.env`, documented in plain text | A secret manager, injected at runtime. The code already reads everything from the environment, so this needs no code change |
| **Compute** | `local[*]` in one container | A real cluster (EMR / Databricks / K8s), with the driver separated from executors |
| **Checkpoints** | A named Docker volume | Durable, replicated object storage (S3/ADLS) ; a lost checkpoint means reprocessing from scratch |
| **Schema changes** | `postgres_setup.sql` re-runs only on an empty volume | Versioned migrations (Alembic / Flyway), applied forward, never by dropping the database |
| **The warehouse** | One PostgreSQL instance | A columnar warehouse for analytics, with Postgres kept for operational lookups. `event_metrics` is already the shape a BI tool wants |
| **Alerting** | `logs/metrics.jsonl` + SQL queries | Metrics shipped to Prometheus/Datadog, with alerts on consumer lag and quarantine-rate spikes ; the two numbers that actually predict trouble |
| **Late data** | Windows accumulate indefinitely | An explicit lateness policy and a documented restatement process for corrections | -->
