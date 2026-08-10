# Test Cases

Manual test plan with **actual recorded results**. Every "Actual" column below is real output from a run executed
on the date given ; nothing here is predicted, and where a first attempt produced a misleading number it is
reported as such rather than quietly re-run (see TC-05).

**Run date:** 2026-08-10 · **Environment:** Spark 4.2.0 / Java 21 / Python 3.13 (`quay.io/jupyter/pyspark-notebook:2026-08-08`), PostgreSQL 16-alpine, Docker Desktop on Windows 11
**Configuration:** `MAX_FILES_PER_TRIGGER=20`, `TRIGGER_INTERVAL=10 seconds`, `SHUFFLE_PARTITIONS=4`
**Starting state:** `make reset` (empty database, no checkpoint, no data files)

## Result summary

| # | Test | Result |
|---|---|---|
| TC-01 | CSV files generated correctly | Pass |
| TC-02 | Spark detects and processes new files | Pass |
| TC-03 | Data transformations are correct | Pass |
| TC-04 | Data written to PostgreSQL without errors | Pass |
| TC-05 | Performance within expected limits | Pass |
| TC-06 | Restart mid-stream produces no duplicates | Pass |
| TC-07 | Malformed rows quarantined with a reason | Pass |
| TC-08 | PostgreSQL unavailable → clear error, not a hang | Pass |
| TC-08b | Recovery after the outage loses no data | Pass |
| TC-09 | Generator stopped → stream idles cleanly | Pass |
| TC-10 | Windowed aggregate matches raw events | Pass |
| TC-11 | Automated test suite passes | Pass |

**11 of 11 passed** (TC-08b counted with TC-08).

### Coverage of the brief's "What to Test"

| The brief asks | Covered by |
|---|---|
| Are the CSV files being generated correctly? | TC-01 |
| Is Spark detecting and processing new files? | TC-02, TC-09 |
| Are the data transformations correct? | TC-03, TC-07 |
| Is data being written into PostgreSQL without errors? | TC-04, TC-06, TC-08, TC-08b |
| Are performance metrics within expected limits? | TC-05 (and `performance_metrics.md` for the full load matrix) |


## TC-01 ; CSV files are generated correctly

**Precondition:** `make reset`, no files in `data/`.
**Steps:** `make generate ARGS="--rate 100 --duration 6 --batch-size 50 --bad-rate 0.10 --seed 42"`, then inspect `data/incoming/`.

| Expected | Actual |
|---|---|
| Files appear in `incoming/` | **12 files** |
| Row count = batches × batch-size | **600 data rows** (12 × 50) |
| Header is exactly the 9 contract columns | `event_id,event_time,generated_at,user_id,product_id,category,event_type,price,quantity` |
| Rows are well-formed | `46685257-…,2026-08-10T10:59:01.655Z,…,clothing,add_to_cart,347.81,5` |
| Malformed rows present at the requested rate | Confirmed ; e.g. `event_time` = `not-a-timestamp` |
| **No partial files left behind** | `data/staging/` contained **0 files** |

**Result: Pass.** The empty `staging/` is the meaningful assertion ; it confirms the atomic
`staging/ → incoming/` publish completed for every file, leaving nothing half-written where Spark could see it.


## TC-02 ; Spark detects and processes new files

**Precondition:** TC-01 complete (12 files waiting), database empty.
**Steps:** `make stream`, observe `logs/pipeline.log`.

| Expected | Actual |
|---|---|
| Source logs the watched directory | `streaming source: watching /home/jovyan/work/data/incoming (max_files_per_trigger=20, …)` |
| Files are picked up without restart | `batch 0: events -> 538 in, 538 inserted, 0 skipped` |
| Valid + quarantined = generated | 538 + 62 = **600** |
| Metrics written in the same batch | `batch 0: metrics -> 10 window/category row(s) upserted` |

**Result: Pass.** All 12 files consumed in one batch (12 ≤ `maxFilesPerTrigger` of 20).


## TC-03 ; Data transformations are correct

**Steps:** SQL assertions against `events` after TC-02.

| Expected | Actual |
|---|---|
| No negative prices survive | **0** |
| No `quantity < 1` survives | **0** |
| `category` normalised (lowercase, trimmed) | **0** unnormalised |
| No NULL `event_id` / `event_time` | **0** / **0** |
| `latency_ms` derived for every row | **0** NULL |
| `revenue = price × quantity` for purchases only | see below |

| event_type | rows | with revenue | wrong revenue |
|---|---|---|---|
| `add_to_cart` | 152 | 0 | 0 |
| `purchase` | 44 | **44** | **0** |
| `view` | 342 | 0 | 0 |

**Result: Pass.** Revenue is derived for exactly the 44 purchases and no others, with zero arithmetic
mismatches against `round(price × quantity, 2)`.


## TC-04 ; Data written to PostgreSQL without errors

| Expected | Actual |
|---|---|
| Every generated event accounted for | 538 + 62 = **600 = 600 generated** |
| No errors in the pipeline log | **0** matches for `ERROR` / `Traceback` / `Exception` |
| Per-batch metrics captured | `logs/metrics.jsonl` written by the listener |

**Result: Pass.** Nothing lost and nothing invented ; the reconciliation is exact, not approximate.


## TC-05 ; Performance within expected limits

> **First attempt produced a misleading number, and it is recorded here rather than discarded.**
> Measuring against TC-01's data gave **p50 = 26,853 ms**, which looks like a serious problem. It is not a
> pipeline result: all 600 events were generated *before* the consumer started, so they sat in a backlog and
> their latency includes the ~20s I spent starting the job. `latency_ms` is measured from the event's own
> timestamp, so a cold-start backlog inflates it by construction. The test was re-run correctly.

**Corrected steps:** with the stream already running, `make generate ARGS="--rate 100 --duration 45 --seed 7"`.

| Metric | Expected | Actual |
|---|---|---|
| p50 latency | ≈ half the 10s trigger + processing | **5,972 ms** |
| p95 latency | ≲ one trigger interval + processing | **10,027 ms** |
| max latency | bounded, no runaway tail | **10,227 ms** |
| min latency | > 0 | **972 ms** |
| Rows measured | ; | 4,404 |
| Reconciliation | 4,404 + 96 = 4,500 generated | |
| Throughput | keeps up with 100 ev/s | 780 then 3,624 events/minute |

**Result: Pass.** p50 of 5,972 ms against a 10-second trigger matches the theoretical prediction (a randomly
arriving event waits on average half a trigger interval, ≈5,000 ms, plus processing) and independently
corroborates the 5.7s baseline in [`performance_metrics.md`](performance_metrics.md).


## TC-06 ; Restart mid-stream produces no duplicates

**The single most important test in the project.** Spark's source is at-least-once; this is what proves the sink
turns that into effectively exactly-once.

**Steps:** built a 125-file backlog (12,000 events) with no consumer running, started the job, **`SIGKILL`ed it
mid-drain**, restarted it, and waited for full drain.

| Expected | Actual |
|---|---|
| Every generated event lands | 22,020 + 480 = **22,500 = 22,500 generated** |
| **No duplicate rows** | 22,020 rows / **22,020 distinct `event_id`** |
| Aggregate stays consistent | **0 mismatched** of 50 window/category rows |
| Revenue reconciles | metrics 1,677,927.44 = raw **1,677,927.44** |

**Result: Pass.** Zero replay warnings appeared in the log, which is itself the evidence that the in-flight
batch's transaction **rolled back** rather than committing partially ; the restart re-processed those files
fresh with `0 skipped`.

> A stronger variant of this test was run during development: a `SIGKILL` timed to land while a batch's
> database transaction was open, at 45,000 events. Result: 70/70 windows matched, 3,277,824.60 revenue exact on
> both sides.


## TC-07 ; Malformed rows are quarantined with a reason

**Steps:** inspect `events_quarantine` after TC-02 (`--bad-rate 0.10`).

| rejection_reason | rows |
|---|---|
| `invalid_quantity` | 20 |
| `invalid_price` | 16 |
| `unparseable_event_time` | 14 |
| `missing_product_id` | 12 |
| **Total** | **62** |

| Expected | Actual |
|---|---|
| Bad rows quarantined, not dropped | 62 rows present |
| Every row carries a reason | **0** with NULL `rejection_reason` |
| Rate ≈ requested `--bad-rate 0.10` | 62/600 = **10.33%** |
| All four defect types represented | 4 of 4 |

**Result: Pass.**


## TC-08 ; PostgreSQL unavailable → clear error, not a hang

**Steps:** queued 200 events with no consumer, ran `docker compose stop postgres`, then started the streaming job
under a 180-second timeout.

| Expected | Actual |
|---|---|
| Fails rather than hanging | Exited in **14 seconds** (timeout 180s never reached) |
| Error names the real cause | `psycopg2.OperationalError: could not translate host name "postgres" to address` |
| Our own logging surfaces it | `[ERROR] [streaming]: batch 17 failed` |
| Query terminates loudly | `MicroBatchExecution: Query … terminated with error` |

**Result: Pass.** This also confirms `process_batch` **re-raises** rather than swallowing. Swallowing would let
Spark treat the batch as successful and commit the checkpoint for data that was never written ; silent,
permanent loss. Failing loudly is the correct behaviour.

## TC-08b ; Recovery after the outage

**Steps:** `docker compose start postgres`, wait for healthy, restart the streaming job.

| Expected | Actual |
|---|---|
| The 200 events queued during the outage land | 22,500 → **22,700 = expected** |
| Still no duplicates | 22,218 rows / **22,218 distinct ids** |

**Result: Pass.** Nothing was lost to the outage ; the checkpoint was never committed for the failed batch, so
the files were simply reprocessed.


## TC-09 ; Generator stopped → stream idles cleanly

**Steps:** left the streaming job running with no generator for several minutes.

| Expected | Actual |
|---|---|
| Job stays alive | Process alive, idling |
| Empty polls are silent | No batch output logged for empty triggers |
| No errors accumulate | **0** `ERROR` / `Traceback` in the log |
| Consumed files may linger in `incoming/` | Observed, and expected ; see below |

**Result: Pass.**

> **Known, documented behaviour, not a defect:** Spark's `cleanSource` housekeeping only runs as part of a batch
> that finds **new** files. With the generator stopped, the last consumed files remain in `incoming/`
> un-archived until new data arrives. They are already durably in PostgreSQL ; archiving is purely a
> directory-listing optimisation, and nothing downstream depends on it. This is recorded in the user guide's
> troubleshooting table so it doesn't read as data loss.


## TC-10 ; Windowed aggregate matches raw events

This is the definition of done: does each stored window agree with what the raw `events` table says for
that same minute?

| Scale | Windows compared | Mismatched | Revenue check |
|---|---|---|---|
| 600 events | 10 | **0** | ; |
| 22,500 events (after a mid-drain `SIGKILL`) | 50 | **0** | 1,677,927.44 = 1,677,927.44 |
| 45,000 events | 70 | **0** | 3,277,824.60 = 3,277,824.60 |

Compared with a `FULL OUTER JOIN` and no `LIMIT`, so a window present on one side and missing on the other would
also be caught ; not just a disagreement between matched rows.

**Result: Pass.** Event counts and revenue agree exactly, to the cent, at every scale tested.


## TC-11 ; Automated test suite

| Command | Expected | Actual |
|---|---|---|
| `make verify` (full suite) | all pass | **86 passed** in 37.99s |
| `make test` (unit only, no database) | all pass, integration skipped | **75 passed, 11 deselected** in 29.76s |

**Result: Pass.** The suite includes the flagship idempotency test, the transaction-atomicity test (which
crashes deliberately between the events and metrics writes and asserts both roll back), and the schema-contract
test that keeps `src/schema.py` and `sql/postgres_setup.sql` from drifting apart.
