-- verification_queries.sql
--
-- Run via `make metrics` (docker compose exec postgres psql ... < this file).
-- Every query here is read-only. Two jobs: (1) the ad hoc "is the data
-- really there?" check during manual verification, and (2) the source of
-- every number quoted in docs/performance_metrics.md -- nothing in that
-- report should ever be written from memory instead of read from here.
-- 

\echo '=== Row counts ==='
SELECT
    (SELECT count(*) FROM events)            AS events_count,
    (SELECT count(*) FROM events_quarantine) AS quarantine_count,
    (SELECT count(*) FROM events) + (SELECT count(*) FROM events_quarantine) AS total_ingested;

\echo '=== Quarantine rate ==='
SELECT round(
    100.0 * (SELECT count(*) FROM events_quarantine)
    / NULLIF((SELECT count(*) FROM events) + (SELECT count(*) FROM events_quarantine), 0),
    2
) AS quarantine_rate_pct;

\echo '=== Quarantine breakdown by reason ==='
SELECT
    rejection_reason,
    count(*) AS n,
    round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS pct_of_quarantine
FROM events_quarantine
GROUP BY rejection_reason
ORDER BY n DESC;

-- p50 / p95 / max, NEVER a mean: streaming latency distributions have a long
-- tail (a slow batch, a checkpoint flush, a GC pause), and a mean hides
-- exactly the thing anyone operating this pipeline would actually care about.
\echo '=== End-to-end latency in ms: p50 / p95 / max (see understand.md Phase 8 for why never a mean) ==='
SELECT
    percentile_cont(0.50) WITHIN GROUP (ORDER BY latency_ms) AS p50_ms,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95_ms,
    max(latency_ms) AS max_ms,
    min(latency_ms) AS min_ms,
    count(*) AS n
FROM events;

\echo '=== Throughput: events landed per minute ==='
SELECT date_trunc('minute', ingested_at) AS minute, count(*) AS events
FROM events
GROUP BY 1
ORDER BY 1;

\echo '=== Top categories by event count ==='
SELECT category, count(*) AS events
FROM events
GROUP BY category
ORDER BY events DESC
LIMIT 10;

-- Phase 9, stretch goal: the windowed aggregate. Populated automatically as
-- part of the same streaming query as events/events_quarantine -- no
-- separate flag or process (see src/streaming.py's process_batch).
\echo '=== event_metrics: most recent windows ==='
SELECT window_start, window_end, category, total_revenue, event_count, updated_at
FROM event_metrics
ORDER BY window_start DESC, category
LIMIT 10;

-- The Phase 9 DoD check, in SQL: does a window's stored aggregate match what
-- the raw events table itself says for that same minute? Every window here
-- is accumulated incrementally, per micro-batch, directly from the rows
-- write_events_batch actually inserted (see transforms.aggregate_by_window) --
-- there is no Spark-managed watermark or "closed" state to wait for anymore,
-- so in principle a window's total is always exactly as complete as
-- everything ingested so far. The `window_end < now() - interval '2 minutes'`
-- filter below is kept anyway, but now purely as an OPERATIONAL heuristic for
-- this verification query, not a correctness requirement: it just excludes
-- windows the pipeline may still be actively writing to when you happen to
-- run this, so the comparison isn't confused by a window that's correct but
-- simply not finished yet.
\echo '=== event_metrics cross-check against raw events ==='
SELECT
    em.window_start, em.window_end, em.category,
    em.total_revenue AS metrics_total_revenue,
    raw.raw_total_revenue,
    em.event_count AS metrics_event_count,
    raw.raw_event_count
FROM event_metrics em
JOIN (
    SELECT
        date_trunc('minute', event_time) AS window_start,
        category,
        coalesce(sum(revenue), 0) AS raw_total_revenue,
        count(*) AS raw_event_count
    FROM events
    GROUP BY 1, 2
) raw ON raw.window_start = em.window_start AND raw.category = em.category
WHERE em.window_end < now() - interval '2 minutes'
ORDER BY em.window_start DESC
LIMIT 10;
