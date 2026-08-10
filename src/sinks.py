"""foreachBatch writers: the idempotent upsert into `events`, the simpler
append into `events_quarantine`, and  the accumulating upsert into `event_metrics`.
"""

from contextlib import contextmanager

import psycopg2
from psycopg2.extras import execute_values

from src import config
from src.logger import get_logger

logger = get_logger("sinks")


@contextmanager
def pg_transaction():
    """One psycopg2 transaction, yielding a cursor. Commits on clean exit,
    rolls back on any exception, always closes the connection.

    This exists because of a specific bug, found by reviewing the metrics
    pipeline's first implementation rather than by it failing in a test -- it only shows up on
    a crash in a narrow window, which is exactly the kind of bug that reaches
    production intact.

    The broken version: write_events_batch opened its own connection and
    committed, then write_metrics_batch opened a SECOND connection and
    committed separately. Crash in between, and Spark replays the batch --
    at which point every event in it is already present, ON CONFLICT DO
    NOTHING skips them all, RETURNING yields an empty set, and the metrics
    contribution for that batch is silently gone FOREVER. `events` stays
    perfectly correct while `event_metrics` quietly under-counts, with no
    error anywhere and no way to notice short of the cross-check query in
    sql/verification_queries.sql.

    The mechanism that makes the accumulate-a-delta design replay-safe (see
    write_metrics_batch) is exactly what makes a lost delta unrecoverable:
    the delta is derived from what the INSERT actually did, so once that
    INSERT is committed, the delta can never be re-derived. Sharing one
    transaction closes it completely, in all three cases:

      - crash BEFORE commit: neither write is durable, replay redoes both;
      - crash AFTER commit: both are durable, replay adds nothing to either;
      - partial: impossible, which is the entire point.

    Note the connection is opened before write_events_batch's Spark-side
    staging write runs, which looks like it holds a transaction open across
    seconds of unrelated JDBC work. It does not: psycopg2 issues BEGIN
    lazily, on the first execute() -- until then this is an idle connection,
    not an idle-in-transaction one, so it takes no locks and blocks nothing.
    """
    conn = psycopg2.connect(**config.pg_connect_kwargs())
    try:
        with conn:  # commits on clean exit, rolls back on exception
            with conn.cursor() as cur:
                yield cur
    finally:
        conn.close()


def write_events_batch(events_df, batch_id: int, cur) -> list[dict]:
    """Idempotent upsert into `events`, safe to replay after a crash. Returns
    the rows ACTUALLY, NEWLY inserted this call -- which the metrics
    accumulation depends on directly (see aggregate_by_window's docstring).

    Runs on the CALLER's cursor and does not commit: this write and the
    metrics write derived from its return value have to land together or not
    at all, and pg_transaction's docstring explains exactly what goes wrong
    when they don't.

    There is no streaming JDBC sink -- writeStream.format("jdbc") does not
    exist. foreachBatch hands back an ordinary batch DataFrame, which is what
    makes df.write.jdbc available again.

    The idempotency mechanism: write to a per-batch staging table (fast,
    parallel, disposable), then INSERT ... SELECT ... ON CONFLICT DO NOTHING.
    A replayed batch (Spark re-executing this exact batch_id after a crash
    that happened after the write but before the checkpoint committed)
    re-writes the same staging rows and inserts nothing new. This only works
    because event_id comes from the PRODUCER: regenerating it in
    Spark would give replayed rows fresh ids that conflict with nothing,
    duplicating perfectly instead of being caught.

    RETURNING only yields rows the statement actually affected -- confirmed
    directly: a row that hits ON CONFLICT DO NOTHING is not returned. That is
    exactly what makes the return value here replay-safe for free, with no
    extra bookkeeping: a replayed batch returns an empty list, since every
    row in it already conflicts.

    The staging table name is per-batch, not fixed: two batches sharing one
    name could overwrite each other's staging data. mode="overwrite" also
    means a stale staging table left behind by an earlier crashed attempt at
    this same batch_id is safely replaced, not appended to. The DROP is
    inside the caller's transaction, so a rollback leaves the staging table
    behind -- harmless, since the retry overwrites it.

    Column names for the INSERT are read from events_df.columns at runtime,
    not hardcoded here or relied on via SELECT * -- self-describing, and
    cannot drift from whatever finalize_events() actually produces.
    """
    staging_table = f"staging_events_{batch_id}"

    row_count = events_df.count()
    if row_count == 0:
        logger.info("batch %d: 0 valid events, nothing to write", batch_id)
        return []

    # Low shuffle.partitions (config.SHUFFLE_PARTITIONS) plus this explicit
    # coalesce: df.write.jdbc opens one connection PER PARTITION, and more
    # partitions than Postgres's max_connections would fail the write or take
    # the database down for everything else connected to it.
    #
    # This runs on Spark's own JVM connections, NOT the caller's cursor, so
    # it is committed independently and is NOT rolled back with the rest of
    # the batch. That is fine and deliberate: a staging table is disposable
    # by construction (mode="overwrite" on retry), which is the whole reason
    # the durable INSERT is a separate statement from it.
    events_df.coalesce(config.SHUFFLE_PARTITIONS).write.jdbc(
        config.JDBC_URL, staging_table, mode="overwrite", properties=config.JDBC_PROPERTIES
    )

    columns_sql = ", ".join(events_df.columns)
    cur.execute(
        f"INSERT INTO {config.EVENTS_TABLE} ({columns_sql}) "
        f"SELECT {columns_sql} FROM {staging_table} "
        f"ON CONFLICT (event_id) DO NOTHING "
        f"RETURNING event_time, category, revenue"
    )
    inserted_rows = [{"event_time": r[0], "category": r[1], "revenue": r[2]} for r in cur.fetchall()]
    cur.execute(f"DROP TABLE IF EXISTS {staging_table}")

    inserted = len(inserted_rows)
    skipped = row_count - inserted
    logger.info(
        "batch %d: events -> %d in, %d inserted, %d skipped (already present)",
        batch_id,
        row_count,
        inserted,
        skipped,
    )
    if skipped:
        logger.warning("batch %d: %d row(s) already present -- this batch was replayed", batch_id, skipped)

    return inserted_rows


def write_quarantine_batch(quarantine_df, batch_id: int) -> None:
    """Plain append into events_quarantine.

    Deliberately simpler than write_events_batch: quarantine is a diagnostic
    aid, not the financial record (see sql/postgres_setup.sql's own comment
    on that table), and it cannot key on event_id the way events does anyway
    -- a structurally corrupt line may have no usable event_id at all. An
    occasional duplicate row here on the rare crash-replay path is an
    accepted, already-documented cost of not needing a second idempotent
    upsert path for a table that isn't the source of truth.
    """
    row_count = quarantine_df.count()
    if row_count == 0:
        logger.info("batch %d: 0 quarantined events", batch_id)
        return

    quarantine_df.coalesce(config.SHUFFLE_PARTITIONS).write.jdbc(
        config.JDBC_URL, config.QUARANTINE_TABLE, mode="append", properties=config.JDBC_PROPERTIES
    )
    logger.info("batch %d: %d event(s) quarantined", batch_id, row_count)


def write_metrics_batch(metric_rows: list[dict], batch_id: int, cur) -> None:
    """Upsert into event_metrics (the windowed-aggregate stretch
    goal): ACCUMULATES, deliberately not replaces -- the opposite rule from
    write_events_batch's DO NOTHING, for a real reason, not an inconsistency.
    """
    if not metric_rows:
        logger.info("batch %d: 0 metric rows, nothing to write", batch_id)
        return

    values = [
        (row["window_start"], row["window_end"], row["category"], row["total_revenue"], row["event_count"])
        for row in metric_rows
    ]

    execute_values(
        cur,
        f"INSERT INTO {config.METRICS_TABLE} "
        "(window_start, window_end, category, total_revenue, event_count, updated_at) "
        "VALUES %s "
        "ON CONFLICT (window_start, window_end, category) DO UPDATE SET "
        f"total_revenue = {config.METRICS_TABLE}.total_revenue + EXCLUDED.total_revenue, "
        f"event_count = {config.METRICS_TABLE}.event_count + EXCLUDED.event_count, "
        "updated_at = EXCLUDED.updated_at",
        values,
        template="(%s, %s, %s, %s, %s, now())",
    )

    logger.info("batch %d: metrics -> %d window/category row(s) upserted", batch_id, len(metric_rows))
