"""Pure DataFrame -> DataFrame transforms: casting, normalising, deriving, and
validating incoming events, ending in a clean/quarantine split.

Nothing here mentions streaming or does any I/O. The exact same functions run
against a real streaming micro-batch DataFrame in production and a 5-row batch
DataFrame in a unit test -- Structured Streaming's whole premise is that batch
and streaming share one API, and this module just takes that promise
seriously. That is what makes every rule below testable in milliseconds,
without Docker-Postgres or a stream running.

aggregate_by_window  is the one deliberate exception to "DataFrame ->
DataFrame" -- it operates on plain Python dicts instead. See its own
docstring for why: a real architectural finding from this phase, not a
stylistic choice.
"""

from datetime import timedelta
from decimal import Decimal

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src import schema
from src.schema import CORRUPT_RECORD_COLUMN

# Matches sql/postgres_setup.sql's events.price/events.revenue columns exactly,
# so their output needs no further conversion before finalize_events hands it
# to the JDBC writer.
_PRICE_PRECISION, _PRICE_SCALE = 10, 2
_REVENUE_PRECISION, _REVENUE_SCALE = 12, 2


def cast_types(df: DataFrame) -> DataFrame:
    """Adds typed columns alongside the original strings -- price_num,
    quantity_num, event_time_ts, generated_at_ts -- rather than overwriting
    them in place.

    Uses SQL try_cast, NOT plain CAST/Column.cast() -- confirmed directly,
    the hard way, that this is not optional. Spark's ANSI SQL mode is ON BY
    DEFAULT as of Spark 4.x (spark.conf.get("spark.sql.ansi.enabled") ==
    "true" here), and under ANSI semantics an invalid CAST *raises*
    (SparkDateTimeException: CAST_INVALID_INPUT for 'not-a-timestamp', for
    instance) instead of quietly returning NULL. A plain .cast() would have
    meant a single bad value anywhere in a micro-batch crashes the ENTIRE
    batch -- the opposite of what a quarantine design is supposed to
    guarantee, since one malformed row would take the whole streaming query
    down rather than being isolated and routed to quarantine. try_cast keeps
    ANSI's other protections (real overflow/divide-by-zero errors elsewhere)
    intact and only relaxes this one, specific, expected failure mode.
    pyspark.sql.functions has no try_cast() of its own (confirmed: it raises
    AttributeError) -- F.expr(...) is the correct way to reach it from Python.

    Keeping the original string columns alongside the typed ones matters for
    two reasons: add_rejection_reason needs the typed column's nullness to
    detect a bad value, and the quarantine path (finalize_quarantine) needs
    the real original string intact to show what was actually received --
    overwriting in place would destroy exactly the evidence a quarantine
    table exists to preserve.
    """
    return (
        df.withColumn("price_num", F.expr(f"try_cast(price AS decimal({_PRICE_PRECISION},{_PRICE_SCALE}))"))
        .withColumn("quantity_num", F.expr("try_cast(quantity AS int)"))
        .withColumn("event_time_ts", F.expr("try_cast(event_time AS timestamp)"))
        .withColumn("generated_at_ts", F.expr("try_cast(generated_at AS timestamp)"))
    )


def normalise(df: DataFrame) -> DataFrame:
    """Trims and lowercases event_type/category so "Purchase" and "purchase"
    (or stray whitespace) don't become two distinct values downstream. Spark's
    string functions propagate NULL through unchanged, so a null category
    stays null rather than becoming the literal string "none" or similar.
    """
    return df.withColumn("event_type", F.trim(F.lower(F.col("event_type")))).withColumn(
        "category", F.trim(F.lower(F.col("category")))
    )


def derive(df: DataFrame) -> DataFrame:
    """Adds revenue (purchases only), ingested_at, and latency_ms.

    current_timestamp() is evaluated once per query execution in Spark, not
    once per row, so every row in the same micro-batch gets the same
    ingested_at -- exactly right, since the meaningful question is "when was
    this batch processed", not a per-row instant.

    revenue is explicitly cast to decimal(12,2), matching events.revenue in
    sql/postgres_setup.sql exactly -- confirmed directly that Spark's decimal
    multiplication rule does NOT just reuse price_num's decimal(10,2): a
    decimal(10,2) times an int widens to decimal(21,2) (Spark's standard
    precision-growth rule for decimal arithmetic, precision1+precision2+1).
    Postgres would very likely coerce that down to NUMERIC(12,2) implicitly on
    insert anyway given how small real revenue values are here, but relying on
    that coercion silently succeeding is exactly the kind of assumption this
    project verifies instead of trusting -- the explicit cast makes Spark's
    own dtype match the declared column type, the same discipline already
    applied to price_num/quantity_num in cast_types.
    """
    return (
        df.withColumn("ingested_at", F.current_timestamp())
        .withColumn(
            "revenue",
            F.when(
                F.col("event_type") == "purchase", F.col("price_num") * F.col("quantity_num")
            ).cast(f"decimal({_REVENUE_PRECISION},{_REVENUE_SCALE})"),
        )
        .withColumn(
            "latency_ms",
            F.unix_millis(F.col("ingested_at")) - F.unix_millis(F.col("generated_at_ts")),
        )
    )


def add_rejection_reason(df: DataFrame) -> DataFrame:
    """One rejection_reason column, one condition per row, first match wins --
    the quarantine pattern from understand.md Phase 6, one pass, no duplicated
    rule logic.

    Order matters: a structurally corrupt line is checked first, since once a
    line's own shape is broken, whatever a downstream field looks like is a
    symptom of that, not an independent problem worth its own reason.

    Confirmed directly rather than assumed, more than once:
      - an empty CSV field reads back as SQL NULL, not an empty string -- but the required-string checks below still test for
        both NULL and "" explicitly anyway, rather than leaning on that one
        reader default forever: it is real, upstream, reader-configuration
        behavior this module has no control over, not a property of the data
        itself, and a rule this important is worth making true on its own
        terms rather than true only because of what a different module's
        options happen to be set to today.
      - `~col.isin(...)` on a NULL column evaluates to NULL, not true -- and a
        NULL when() condition does not fire, so a naive "not a known
        event_type" check would let a genuinely null event_type through
        completely undetected. Every null-sensitive condition here is
        structured as `isNull() | <comparison>` specifically so the null case
        is never left to a comparison operator's three-valued logic.
    """
    return df.withColumn(
        "rejection_reason",
        F.when(F.col(CORRUPT_RECORD_COLUMN).isNotNull(), "structurally_corrupt_line")
        .when(F.col("event_id").isNull() | (F.col("event_id") == ""), "missing_event_id")
        .when(F.col("user_id").isNull() | (F.col("user_id") == ""), "missing_user_id")
        .when(
            F.col("event_type").isNull() | ~F.col("event_type").isin(*schema.EVENT_TYPES),
            "unknown_event_type",
        )
        .when(F.col("product_id").isNull() | (F.col("product_id") == ""), "missing_product_id")
        .when(F.col("event_time_ts").isNull(), "unparseable_event_time")
        .when(F.col("generated_at_ts").isNull(), "unparseable_generated_at")
        .when(F.col("price_num").isNull() | (F.col("price_num") < 0), "invalid_price")
        .when(F.col("quantity_num").isNull() | (F.col("quantity_num") < 1), "invalid_quantity")
        .otherwise(F.lit(None).cast("string")),
    )


def split_valid_invalid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """One pass, two outputs: both halves filter the SAME rejection_reason
    column add_rejection_reason wrote, so the rule logic is never duplicated
    between the two paths."""
    valid = df.filter(F.col("rejection_reason").isNull())
    invalid = df.filter(F.col("rejection_reason").isNotNull())
    return valid, invalid


def finalize_events(df: DataFrame) -> DataFrame:
    """Shapes the VALID side into exactly sql/postgres_setup.sql's `events`
    column set: promotes the typed columns from cast_types to their canonical
    (Postgres) names, and drops everything that table has no column for
    (_corrupt_record, rejection_reason, and the original string forms of the
    four cast fields).
    """
    return df.select(
        F.col("event_id"),
        F.col("event_time_ts").alias("event_time"),
        F.col("generated_at_ts").alias("generated_at"),
        F.col("user_id"),
        F.col("product_id"),
        F.col("category"),
        F.col("event_type"),
        F.col("price_num").alias("price"),
        F.col("quantity_num").alias("quantity"),
        F.col("revenue"),
        F.col("ingested_at"),
        F.col("latency_ms"),
    )


def finalize_quarantine(df: DataFrame) -> DataFrame:
    """Shapes the INVALID side into exactly events_quarantine's column set.

    Deliberately keeps the ORIGINAL STRING columns (event_time, generated_at,
    price, quantity), not the typed ones from cast_types -- events_quarantine
    exists to show what was actually received, and the typed columns are
    frequently NULL or meaningless for a rejected row (that is often exactly
    why it was rejected in the first place).

    quarantined_at reuses ingested_at rather than a fresh current_timestamp()
    call: within one micro-batch they are the same instant either way (see
    derive()'s docstring), so a second call would be redundant, not different.
    """
    return df.select(
        F.col("event_id"),
        F.col("event_time"),
        F.col("generated_at"),
        F.col("user_id"),
        F.col("product_id"),
        F.col("category"),
        F.col("event_type"),
        F.col("price"),
        F.col("quantity"),
        F.col("rejection_reason"),
        F.col(CORRUPT_RECORD_COLUMN).alias("raw_line"),
        F.col("ingested_at").alias("quarantined_at"),
    )


def aggregate_by_window(rows: list[dict]) -> list[dict]:
    """the stretch goal: per-minute, per-category revenue and
    event-count aggregation -- the one thing that gets this project past
    "lands raw events" into genuine continuous analytics.

    The one deliberate exception to this module's own "Spark DataFrame ->
    DataFrame" charter: this takes and returns plain Python dicts, not a
    Spark DataFrame. That is a direct consequence of a real architectural
    finding from this phase, not a stylistic choice -- see streaming.py's
    process_batch and sinks.write_events_batch for the full story. In short:
    the ORIGINAL design ran a second, independent streaming query reading
    Spark's own event archive to aggregate continuously with a watermark.
    Confirmed directly, the hard way: that query's directory listing cost
    grows with the TOTAL cumulative archive size forever, and at just a few
    hundred files it was already unable to keep up with its own trigger
    interval. There is no clean fix for that within the file-source model --
    it is the same "listing is O(files) forever" problem understand.md's already names, just relocated rather than solved.

    The fix: aggregate PER MICRO-BATCH, from the rows write_events_batch just
    ACTUALLY, NEWLY inserted (via its own INSERT ... RETURNING), and
    ACCUMULATE into event_metrics with a running-total upsert instead of
    Spark's own stateful, watermarked aggregation. This needs no second
    query, no second source, and no unbounded listing -- it rides entirely on
    the main query's own already-solved (cleanSource=archive) read path.
    It also solves replay-safety almost for free: a replayed batch's
    RETURNING clause yields zero rows for anything already inserted (the
    same event_id-keyed ON CONFLICT DO NOTHING that makes events itself
    idempotent), so this function never even sees a duplicated contribution
    to increment twice.

    ALMOST free, not free -- worth being precise about, because the gap it
    leaves is subtle and was found by reviewing this design rather than by a
    test failing. Deriving the delta from "what the INSERT actually did"
    means the delta cannot be re-derived once that INSERT has committed on
    its own: a crash after the events write commits but before the metrics
    write does would make the replay see zero returned rows and silently
    drop that batch's revenue forever. The events write and the metrics
    write therefore share ONE transaction (sinks.pg_transaction), which is
    what makes the two states impossible to separate. That is a hard
    requirement of this design, not a nicety.

    Event time, not processing time: bucketing by event_time (when the click
    happened) rather than arrival time is what makes the result *correct*
    rather than merely *timely* -- a network-delayed event still belongs to
    the minute it actually occurred in.

    No watermark here, and deliberately so -- not an oversight. A watermark's
    only job is telling Spark when it is safe to drop a window's IN-MEMORY
    state. Since nothing here is held in Spark's memory at all (every batch's
    partial contribution is flushed straight to Postgres via write_metrics_batch
    before this function is ever called again), there is no state to
    protect and nothing for a watermark to bound.
    """
    buckets: dict[tuple, dict] = {}
    for row in rows:
        event_time = row["event_time"]
        window_start = event_time.replace(second=0, microsecond=0)
        window_end = window_start + timedelta(minutes=1)
        key = (window_start, window_end, row["category"])

        bucket = buckets.setdefault(
            key,
            {
                "window_start": window_start,
                "window_end": window_end,
                "category": row["category"],
                "total_revenue": Decimal("0.00"),
                "event_count": 0,
            },
        )
        bucket["total_revenue"] += row["revenue"] or Decimal("0.00")
        bucket["event_count"] += 1

    return list(buckets.values())
