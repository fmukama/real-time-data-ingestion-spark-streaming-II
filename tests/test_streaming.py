"""Tests for src/streaming.py.

Uses Trigger.availableNow + a memory sink for the read-path tests: this is
what makes a streaming query testable at all -- a normal streaming query
never terminates, but availableNow drains whatever already exists in the
source and then stops, so query.awaitTermination() returns and the test
finishes in bounded time.

The archiving test needs a real running (processingTime-triggered) query
across at least two batches instead -- confirmed directly while building this
phase that cleanSource's cleanup executes on a SUBSEQUENT batch's bookkeeping,
never the same batch that consumed the file, so a single availableNow drain
never archives its own last file no matter how the cleanup delay is tuned.

No Postgres needed for any of this -- just a real SparkSession, which is
already how every test in this suite runs (inside the spark container).
"""

import os
import time
from datetime import datetime, timezone

import pytest

from src.generator import make_event, write_batch
from src.schema import CORRUPT_RECORD_COLUMN, CSV_COLUMNS
from src.streaming import build_source

_FROZEN_NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

# spark fixture: tests/conftest.py (session-scoped, shared across the suite)


@pytest.fixture
def dirs(tmp_path):
    incoming, archive, checkpoint = tmp_path / "incoming", tmp_path / "archive", tmp_path / "checkpoint"
    incoming.mkdir()
    archive.mkdir()
    checkpoint.mkdir()
    return incoming, archive, checkpoint


def _drain_available_now(df, checkpoint_dir, query_name):
    """Start a memory-sink query with Trigger.availableNow, wait for it to
    finish draining, and return the resulting in-memory table as a batch
    DataFrame. The one deterministic, finite way to test a streaming source.
    """
    query = (
        df.writeStream.format("memory")
        .queryName(query_name)
        .outputMode("append")
        .trigger(availableNow=True)
        .option("checkpointLocation", str(checkpoint_dir))
        .start()
    )
    query.awaitTermination()
    return query


def test_importing_streaming_does_not_mutate_the_shared_event_schema():
    """StructType.add() mutates its receiver in place and returns that same
    object rather than a copy -- confirmed directly while building this phase,
    the hard way (it silently turned EVENT_SCHEMA into a 10-field schema the
    moment src.streaming was imported anywhere in the process). _READER_SCHEMA
    must instead be built from a fresh list, leaving schema.EVENT_SCHEMA and
    CSV_COLUMNS untouched. src.streaming is already imported at the top of this
    file -- if it mutated EVENT_SCHEMA, it already would have by now.
    """
    from src import schema as schema_module

    assert [f.name for f in schema_module.EVENT_SCHEMA.fields] == schema_module.CSV_COLUMNS
    assert CORRUPT_RECORD_COLUMN not in schema_module.CSV_COLUMNS


def test_build_source_reads_valid_events_end_to_end(spark, dirs):
    incoming, archive, checkpoint = dirs
    from faker import Faker

    fake = Faker()
    fake.seed_instance(1)
    events = [make_event(fake, now=_FROZEN_NOW) for _ in range(5)]
    staging = incoming.parent / "staging"
    staging.mkdir(exist_ok=True)
    write_batch(events, staging, incoming)

    df = build_source(spark, incoming_dir=str(incoming), archive_dir=str(archive), max_files_per_trigger=20)
    _drain_available_now(df, checkpoint, "read_test")

    result = spark.sql("SELECT * FROM read_test").orderBy("user_id").collect()
    assert len(result) == 5
    assert {row["event_id"] for row in result} == {e["event_id"] for e in events}
    assert all(row[CORRUPT_RECORD_COLUMN] is None for row in result)


def test_build_source_captures_a_structurally_corrupt_line(spark, dirs):
    incoming, archive, checkpoint = dirs
    with open(incoming / "manual.csv", "w", newline="", encoding="utf-8") as f:
        f.write(",".join(CSV_COLUMNS) + "\n")
        f.write("id-good,2026-01-01T00:00:00.000Z,2026-01-01T00:00:00.000Z,user-1,prod-1,books,view,10.00,1\n")
        # One field short -- a structurally broken line, not a business-rule
        # violation. Confirmed directly: Spark fills what it can read
        # positionally (up to price) and only nulls the missing trailing
        # field (quantity), while still capturing the raw line.
        f.write("id-bad,2026-01-01T00:00:00.000Z,2026-01-01T00:00:00.000Z,user-2,prod-2,books,view,10.00\n")

    df = build_source(spark, incoming_dir=str(incoming), archive_dir=str(archive), max_files_per_trigger=20)
    _drain_available_now(df, checkpoint, "corrupt_test")

    rows = {row["event_id"]: row for row in spark.sql("SELECT * FROM corrupt_test").collect()}
    assert len(rows) == 2, "the corrupt line must still surface as a row, not be silently dropped"
    assert rows["id-good"][CORRUPT_RECORD_COLUMN] is None
    assert rows["id-bad"][CORRUPT_RECORD_COLUMN] is not None
    assert rows["id-bad"][CORRUPT_RECORD_COLUMN].startswith("id-bad,")
    assert rows["id-bad"]["quantity"] is None  # the one field genuinely missing


def test_build_source_uses_max_files_per_trigger_to_bound_a_batch(spark, dirs):
    """Writes more files than max_files_per_trigger allows and confirms the
    FIRST batch reads only the bounded number -- the actual backpressure
    guarantee, not just that the option was passed through."""
    incoming, archive, checkpoint = dirs
    from faker import Faker

    fake = Faker()
    fake.seed_instance(2)
    staging = incoming.parent / "staging"
    staging.mkdir(exist_ok=True)
    for i in range(5):
        write_batch([make_event(fake, now=_FROZEN_NOW)], staging, incoming)

    df = build_source(spark, incoming_dir=str(incoming), archive_dir=str(archive), max_files_per_trigger=2)
    query = (
        df.writeStream.format("memory")
        .queryName("backpressure_test")
        .outputMode("append")
        .trigger(processingTime="1 second")
        .option("checkpointLocation", str(checkpoint))
        .start()
    )
    time.sleep(1.5)  # let exactly one micro-batch fire
    progress = query.recentProgress
    query.stop()

    assert progress, "expected at least one micro-batch to have run"
    assert progress[0]["numInputRows"] <= 2, (
        f"maxFilesPerTrigger=2 (1 event/file) should cap the first batch at 2 rows, got {progress[0]['numInputRows']}"
    )


def test_build_source_archives_consumed_files_across_a_later_batch(spark, dirs):
    """The real, verified behavior: archiving lands under
    <archive_dir>/<absolute source path> (not flatly in archive_dir), and only
    completes once a SUBSEQUENT batch has run -- so this writes a second file
    specifically to give the first one a chance to be archived, rather than
    asserting archiving after only one batch (which would never pass)."""
    incoming, archive, checkpoint = dirs
    from faker import Faker

    fake = Faker()
    fake.seed_instance(3)
    staging = incoming.parent / "staging"
    staging.mkdir(exist_ok=True)
    first_path = write_batch([make_event(fake, now=_FROZEN_NOW)], staging, incoming)

    df = build_source(spark, incoming_dir=str(incoming), archive_dir=str(archive), max_files_per_trigger=20)
    query = (
        df.writeStream.format("memory")
        .queryName("archive_test")
        .outputMode("append")
        .trigger(processingTime="1 second")
        .option("checkpointLocation", str(checkpoint))
        .start()
    )

    archived = []
    for _ in range(10):
        time.sleep(1)
        write_batch([make_event(fake, now=_FROZEN_NOW)], staging, incoming)  # keep giving it a "next" batch
        archived = [os.path.join(r, f) for r, _, fs in os.walk(archive) for f in fs if f == first_path.name]
        if archived:
            break
    query.stop()

    assert archived, f"{first_path.name} was never archived under {archive} across 10 subsequent batches"
    assert not (incoming / first_path.name).exists()
