"""Wires source -> transforms -> sinks: the file-based streaming source
(build_source), the foreachBatch callback that assembles the full pipeline for
one micro-batch (process_batch), and the function that starts the real query
(start_query).
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StringType, StructField, StructType

from src import config, schema
from src.logger import get_logger
from src.monitoring import register as register_metrics_listener
from src.schema import CORRUPT_RECORD_COLUMN
from src.sinks import pg_transaction, write_events_batch, write_metrics_batch, write_quarantine_batch
from src.transforms import (
    add_rejection_reason,
    aggregate_by_window,
    cast_types,
    derive,
    finalize_events,
    finalize_quarantine,
    normalise,
    split_valid_invalid,
)

logger = get_logger("streaming")

# Derived from EVENT_SCHEMA, not equal to it. EVENT_SCHEMA is the
# generator/Postgres contract and must stay exactly the 9 real columns --
# CSV_COLUMNS is generated from it, and the generator never writes this extra
# column. This reader-only schema appends it purely so Spark has somewhere to
# put a corrupt line's text.
#
# Built via StructType(list(...) + [...]), NOT schema.EVENT_SCHEMA.add(...).
# Confirmed directly, the hard way: StructType.add() mutates the receiver IN
# PLACE and returns that same object -- it is not a builder that hands back a
# copy. Calling it on schema.EVENT_SCHEMA would corrupt the shared singleton
# the moment this module is imported anywhere, silently appending
# _corrupt_record to the real event contract every other module (and
# CSV_COLUMNS) relies on. list(...) + [...] creates a genuinely new list, so
# the StructType built from it shares no mutable state with EVENT_SCHEMA.
_READER_SCHEMA = StructType(
    list(schema.EVENT_SCHEMA.fields) + [StructField(CORRUPT_RECORD_COLUMN, StringType(), nullable=True)]
)


def build_source(
    spark: SparkSession,
    *,
    incoming_dir: str | None = None,
    archive_dir: str | None = None,
    max_files_per_trigger: int | None = None,
) -> DataFrame:
    """The file-based streaming source.

    incoming_dir/archive_dir/max_files_per_trigger default to config's global
    paths but are overridable so tests can point at a tmp_path instead of
    fighting global state.
    """
    incoming_dir = incoming_dir or config.INCOMING_DIR
    archive_dir = archive_dir or config.ARCHIVE_DIR
    max_files_per_trigger = max_files_per_trigger or config.MAX_FILES_PER_TRIGGER

    logger.info(
        "streaming source: watching %s (max_files_per_trigger=%d, archiving consumed files under %s)",
        incoming_dir,
        max_files_per_trigger,
        archive_dir,
    )

    return (
        spark.readStream
        # Explicit schema, matched by POSITION not name -- why
        # CSV_COLUMNS/EVENT_SCHEMA staying in lockstep matters this much.
        .schema(_READER_SCHEMA)
        .option("header", "true")
        # Bounds how many files one micro-batch consumes. Without this, the
        # FIRST batch after any backlog (a restart, a slow consumer) tries to
        # eat everything sitting in incoming_dir at once -- a possible OOM,
        # and certainly a multi-minute first-batch latency.
        .option("maxFilesPerTrigger", max_files_per_trigger)
        # The file source lists the ENTIRE watched directory every trigger --
        # O(files-in-directory), forever, if nothing is ever removed. Archiving
        # consumed files keeps that listing cheap indefinitely.
        #
        # Two things confirmed directly, not assumed, and worth knowing before
        # relying on either: (1) an archived file lands at
        # <archive_dir>/<the source file's own absolute path>, not flatly in
        # archive_dir -- Spark mirrors the full source path under the archive
        # root, which means archive_dir itself grows forever and is NOT a
        # cheap thing to list or recurse into at scale (confirmed directly in
        #  a second reader over that directory became unable to keep
        # up with its own trigger interval at just a few hundred files -- see
        # transforms.aggregate_by_window's docstring); (2) cleanup runs as
        # part of a SUBSEQUENT batch that finds NEW files to process -- never
        # the same batch that consumed the file, and never an EMPTY poll
        # either, no matter how many idle trigger cycles tick by. A one-shot
        # drain (Trigger.availableNow with nothing else arriving afterward)
        # will never archive its own last batch, and even a
        # continuously-running query only archives what it already committed
        # once genuinely new data arrives -- a real, if usually brief, lull in
        # ingestion is enough to leave the last batch or two sitting
        # un-archived until the next file shows up. Not a problem for the
        # events THEMSELVES (already durably in Postgres well before
        # archiving ever runs) -- archiving here is purely a directory-listing
        # optimization for THIS source, nothing downstream depends on it
        # having happened. archive_dir must not be inside incoming_dir either,
        # or archived files get rediscovered forever.
        .option("cleanSource", "archive")
        .option("sourceArchiveDir", archive_dir)
        # A structurally broken line (wrong field count) is captured into
        # CORRUPT_RECORD_COLUMN with its original text rather than silently
        # dropped by the parser before any of our own validation ever runs.
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COLUMN)
        .csv(incoming_dir)
    )


def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    """The actual foreachBatch callback: runs one micro-batch through the full
    transform pipeline, writes both halves of the split,
    and accumulates this batch's contribution to the windowed aggregate
     -- all from the SAME source read, the SAME transform pass, and
    the SAME query.

    This is the one place the whole pipeline is assembled -- build_source
    only knows about files, transforms.py only knows about DataFrame rules,
    sinks.py only knows how to write already-shaped input. Nothing else needs
    to change if any one of those three pieces changes internally.

    Wrapped in a top-level try/except that logs the full traceback and
    RE-RAISES. Swallowing the exception here would make Spark consider the
    batch successfully processed -- committing the checkpoint -- even though
    the data was never durably written anywhere: a silent, permanent loss
    with no error surfaced at all. Re-raising is what makes Spark retry the
    batch (or fail the query loudly) instead of quietly moving on.

    processed.persist() matters more than it looks like it should. Confirmed
    directly while building metrics, the hard way: write_events_batch
    and write_quarantine_batch each call BOTH .count() and .write.jdbc(...) on
    their own argument -- two actions apiece. Without persisting, Spark's
    lazy evaluation means EVERY one of those actions re-executes the ENTIRE
    lineage from scratch, including re-reading and re-parsing the source
    files -- observed directly as the metrics listener reporting
    numInputRows=60 for a batch a controlled test had written exactly 20 rows
    into. That is not just a cosmetic metrics-accuracy problem: it is 3x the
    file I/O and re-parsing this pipeline actually needs per batch. Persisting
    processed once, right before the split both branches read from, means
    only the FIRST action anywhere downstream (on either branch) pays for the
    source read and the cast/normalise/derive/rejection-reason chain; every
    action after that reuses the cached result. unpersist() in a finally block
    releases it regardless of whether the batch succeeded or raised.

    The windowed aggregate reads write_events_batch's OWN return
    value (the rows it actually, newly inserted), not the `valid` DataFrame
    directly -- that is what makes the aggregate replay-safe for free,
    without a second idempotency mechanism of its own. See
    transforms.aggregate_by_window and sinks.write_metrics_batch for why.

    Those two writes share ONE transaction (sinks.pg_transaction), and that
    is load-bearing, not tidiness. The metrics delta is derived from what the
    events INSERT actually did, so it cannot be re-derived once that INSERT
    has committed by itself -- committing them separately means a crash in
    between silently and permanently under-counts event_metrics while
    leaving `events` perfectly correct. pg_transaction's docstring has the
    full failure walkthrough.

    Quarantine deliberately sits OUTSIDE that transaction. It cannot join it
    anyway -- it is a Spark JDBC append on the JVM's own connections, not a
    psycopg2 statement -- and it is the diagnostic table, not the financial
    record (sql/postgres_setup.sql says so on the table itself). Running it
    after the commit also keeps the transaction as short as it can be.
    """
    try:
        processed = add_rejection_reason(derive(normalise(cast_types(batch_df))))
        processed.persist()
        try:
            valid, invalid = split_valid_invalid(processed)
            with pg_transaction() as cur:
                inserted_rows = write_events_batch(finalize_events(valid), batch_id, cur)
                write_metrics_batch(aggregate_by_window(inserted_rows), batch_id, cur)
            write_quarantine_batch(finalize_quarantine(invalid), batch_id)
        finally:
            processed.unpersist()
    except Exception:
        logger.exception("batch %d failed", batch_id)
        raise


def start_query(spark: SparkSession, *, trigger_interval: str | None = None) -> StreamingQuery:
    """Builds the source and starts the real streaming query: process_batch
    via foreachBatch, on a fixed processingTime trigger, checkpointed to the
    named Docker volume (config.EVENTS_CHECKPOINT -- see src/config.py for why
    not the bind-mounted working tree). Also registers the metrics listener
    so every real run captures logs/metrics.jsonl automatically,
    without a caller needing to remember a separate step.
    """
    trigger_interval = trigger_interval or config.TRIGGER_INTERVAL
    register_metrics_listener(spark)
    events = build_source(spark)
    return (
        events.writeStream.foreachBatch(process_batch)
        .outputMode("append")
        .trigger(processingTime=trigger_interval)
        .option("checkpointLocation", config.EVENTS_CHECKPOINT)
        .start()
    )
