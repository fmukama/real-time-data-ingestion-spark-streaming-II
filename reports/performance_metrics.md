# Performance Metrics

Every number below is measured, not estimated ; read from `sql/verification_queries.sql` against real data produced
by real runs of `data_generator.py` -> `spark_streaming_to_postgres.py` -> PostgreSQL, or from
`logs/metrics.jsonl`, the per-micro-batch progress record `src/monitoring.py`'s `MetricsListener` captures for
every batch Spark processes.


## Load matrix

Four scenarios, each varying exactly one parameter from the baseline ; the standard way to make each result
attributable to a single cause rather than a tangle of simultaneous changes. `bad_rate` held fixed at the
generator's own default (2%) throughout, so quarantine rate stays a controlled constant and every latency
difference is attributable to load/configuration, not to a shifting defect rate.

| Scenario | Generator rate | `maxFilesPerTrigger` | Trigger interval | Duration | Changed vs. baseline |
|---|---|---|---|---|---|
| Baseline | 100 events/s | 20 | 10s | 90s | ; |
| High rate | 500 events/s | 20 | 10s | 60s | generator rate only |
| Tight trigger | 100 events/s | 20 | 2s | 90s | trigger interval only |
| Small `maxFilesPerTrigger` | 100 events/s | **2** | 10s | 90s | `maxFilesPerTrigger` only |

Each run: `make reset` (clean checkpoint + empty tables) -> `make stream` (with the scenario's env override where
applicable) -> `make generate` with the scenario's flags -> polled until every generated event was durably accounted
for in `events` + `events_quarantine` -> `make metrics` captured to `logs/verify_<scenario>.txt`.


## Headline results

| Scenario | Total events | Quarantine rate | p50 latency | p95 latency | Max latency |
|---|---|---|---|---|---|
| Baseline | 9,000 | 2.01% | **5.7s** | 10.4s | 10.7s |
| High rate | 30,000 | 1.86% | **53.9s** | 95.7s | 98.7s |
| Tight trigger | 9,000 | 1.74% | **1.9s** | 2.4s | 5.9s |
| Small `maxFilesPerTrigger` | 9,000 | 1.86% | **192.5s** | 352.9s | 369.8s |

(Full per-scenario SQL output: `logs/verify_baseline.txt`, `verify_high_rate.txt`, `verify_tight_trigger.txt`,
`verify_small_max_files.txt`.)

Reported as **p50 / p95 / max, never a mean** ; streaming latency has a long tail (a slow batch, a checkpoint
flush, a backlog), and a mean would hide exactly the thing worth knowing. `latency_ms` itself is
`ingested_at − generated_at`, a real column on every row in `events` (understand.md Phase 3/6), not a derived or
sampled estimate.
