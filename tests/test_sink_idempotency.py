"""The flagship test: proves write_events_batch is genuinely
idempotent under replay, not just "probably fine" by construction. Also
covers write_quarantine_batch's deliberately weaker guarantee, and (Phase 9,
the windowed-aggregate stretch goal) write_metrics_batch's accumulating
upsert plus the transaction that binds it to the events write.

Needs a live Postgres, so every test here is @pytest.mark.integration --
excluded from `make test` (the fast unit suite), included in `make verify`.
Uses the real spark/pg_conn/clean_tables fixtures from tests/conftest.py, and
the real transform pipeline from src/transforms.py, so this exercises the
exact code path production uses -- not a simplified stand-in for it.
"""

from datetime import datetime, timezone
from decimal import Decimal

import pytest
from faker import Faker

from src import config
from src.generator import make_event
from src.schema import CORRUPT_RECORD_COLUMN, CSV_COLUMNS
from src.sinks import (
    pg_transaction,
    write_events_batch,
    write_metrics_batch,
    write_quarantine_batch,
)
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

pytestmark = pytest.mark.integration

_FROZEN_NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def _events_df(spark, rows):
    columns = CSV_COLUMNS + [CORRUPT_RECORD_COLUMN]
    tuples = [tuple((row.get(c) or None) for c in columns) for row in rows]
    return spark.createDataFrame(tuples, ", ".join(f"{c} string" for c in columns))


def _process(df):
    df = cast_types(df)
    df = normalise(df)
    df = derive(df)
    df = add_rejection_reason(df)
    return df


def _events_row_count(pg_conn):
    with pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {config.EVENTS_TABLE}")
            return cur.fetchone()[0]


def test_write_events_batch_is_idempotent_under_replay(spark, pg_conn, clean_tables):
    """The core guarantee Phase 7 exists for: Spark re-executing the SAME
    batch_id after a crash between the write and the checkpoint commit must
    not duplicate rows. This directly simulates that replay -- identical
    batch_id, identical data, called twice.
    """
    fake = Faker()
    fake.seed_instance(1)
    events = [make_event(fake, now=_FROZEN_NOW) for _ in range(20)]
    events_out = finalize_events(split_valid_invalid(_process(_events_df(spark, events)))[0])

    with pg_transaction() as cur:
        write_events_batch(events_out, batch_id=0, cur=cur)
    assert _events_row_count(pg_conn) == 20

    with pg_transaction() as cur:
        write_events_batch(events_out, batch_id=0, cur=cur)  # the replay
    assert _events_row_count(pg_conn) == 20, "replaying the same batch_id must not duplicate rows"


def test_write_events_batch_returns_only_the_rows_it_actually_inserted(spark, pg_conn, clean_tables):
    """RETURNING's exact semantics, which Phase 9's metrics accumulation is
    built entirely on top of: a row skipped by ON CONFLICT DO NOTHING is NOT
    returned. Confirmed here directly rather than assumed, because if it were
    ever otherwise, every replayed batch would silently double-count itself
    into event_metrics.
    """
    fake = Faker()
    fake.seed_instance(3)
    events = [make_event(fake, now=_FROZEN_NOW) for _ in range(5)]
    events_out = finalize_events(split_valid_invalid(_process(_events_df(spark, events)))[0])

    with pg_transaction() as cur:
        first = write_events_batch(events_out, batch_id=0, cur=cur)
    assert len(first) == 5, "a first write returns every row"

    with pg_transaction() as cur:
        replayed = write_events_batch(events_out, batch_id=0, cur=cur)
    assert replayed == [], "a replay returns nothing -- every row hit DO NOTHING"


def test_write_events_batch_dedupes_a_repeated_event_id_across_different_batches(spark, pg_conn, clean_tables):
    """Broader than exact replay: ON CONFLICT targets event_id itself, so any
    two rows sharing an id conflict regardless of which batch_id's staging
    table they came through -- not just a byte-identical retry of one batch.
    """
    fake = Faker()
    fake.seed_instance(2)
    batch_1 = [make_event(fake, now=_FROZEN_NOW) for _ in range(3)]
    shared = batch_1[-1]  # will reappear, unchanged, in the second batch
    batch_2 = [shared] + [make_event(fake, now=_FROZEN_NOW) for _ in range(2)]

    with pg_transaction() as cur:
        write_events_batch(
            finalize_events(split_valid_invalid(_process(_events_df(spark, batch_1)))[0]), batch_id=0, cur=cur
        )
    with pg_transaction() as cur:
        write_events_batch(
            finalize_events(split_valid_invalid(_process(_events_df(spark, batch_2)))[0]), batch_id=1, cur=cur
        )

    assert _events_row_count(pg_conn) == 5  # 3 + 3 - 1 shared, not 6


def test_write_events_batch_with_zero_valid_rows_is_a_no_op(spark, pg_conn, clean_tables):
    """A batch that is entirely quarantine (or genuinely empty) must not
    error trying to write/upsert zero rows."""
    bad = make_event(Faker(), now=_FROZEN_NOW)
    bad["event_id"] = None  # missing_event_id -- guaranteed rejected
    df = _process(_events_df(spark, [bad]))
    valid, _ = split_valid_invalid(df)

    with pg_transaction() as cur:
        assert write_events_batch(finalize_events(valid), batch_id=0, cur=cur) == []  # must not raise

    assert _events_row_count(pg_conn) == 0


def test_write_quarantine_batch_appends_rejected_rows(spark, pg_conn, clean_tables):
    bad = make_event(Faker(), now=_FROZEN_NOW)
    bad["quantity"] = "0"  # invalid_quantity
    df = _process(_events_df(spark, [bad]))
    _, invalid = split_valid_invalid(df)

    write_quarantine_batch(finalize_quarantine(invalid), batch_id=0)

    with pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute(f"SELECT rejection_reason FROM {config.QUARANTINE_TABLE}")
            rows = cur.fetchall()
    assert rows == [("invalid_quantity",)]


def test_write_quarantine_batch_is_not_idempotent_by_design(spark, pg_conn, clean_tables):
    """Documents and confirms the deliberate, accepted tradeoff from
    sql/postgres_setup.sql: events_quarantine has no idempotent upsert path
    (it can't reliably key on event_id -- a structurally corrupt line may not
    have one), so replaying a quarantine write DOES duplicate rows. This is
    the cost of keeping that table simple, not an oversight -- this test is
    what makes sure that stays a documented choice and not a silent surprise.
    """
    bad = make_event(Faker(), now=_FROZEN_NOW)
    bad["quantity"] = "0"
    quarantine_out = finalize_quarantine(split_valid_invalid(_process(_events_df(spark, [bad])))[1])

    write_quarantine_batch(quarantine_out, batch_id=0)
    write_quarantine_batch(quarantine_out, batch_id=0)  # the "replay"

    with pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {config.QUARANTINE_TABLE}")
            count = cur.fetchone()[0]
    assert count == 2, "quarantine intentionally has no dedup -- 2 confirms the tradeoff is real, not just assumed"


# --- write_metrics_batch (Phase 9, stretch goal) ---

_WINDOW = "2026-01-01T00:00:10.000Z"  # every event using this shares one 1-minute window


def _metrics_rows(pg_conn):
    with pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute(
                f"SELECT window_start, window_end, category, total_revenue, event_count "
                f"FROM {config.METRICS_TABLE} ORDER BY window_start, category"
            )
            return cur.fetchall()


def _purchase(price, *, category="books", event_time=_WINDOW):
    event = make_event(Faker(), now=_FROZEN_NOW)  # fresh, unseeded -> a unique event_id
    event.update(event_type="purchase", price=price, quantity="1", category=category, event_time=event_time)
    return event


def _write_full_batch(spark, events, batch_id):
    """Exactly what streaming.process_batch does for the events+metrics half
    of a micro-batch: both writes, ONE transaction, metrics derived from what
    the events INSERT actually inserted. Tests below go through this rather
    than calling write_metrics_batch with hand-made input, so they exercise
    the real coupling between the two writes and not a stand-in for it.
    """
    valid, _ = split_valid_invalid(_process(_events_df(spark, events)))
    with pg_transaction() as cur:
        inserted = write_events_batch(finalize_events(valid), batch_id, cur=cur)
        write_metrics_batch(aggregate_by_window(inserted), batch_id, cur=cur)
    return inserted


def test_write_metrics_batch_writes_correctly_on_first_write(spark, pg_conn, clean_tables):
    event = make_event(Faker(), now=_FROZEN_NOW)
    event.update(event_type="view", category="garden", event_time=_WINDOW)

    _write_full_batch(spark, [event], batch_id=0)

    rows = _metrics_rows(pg_conn)
    assert len(rows) == 1
    assert rows[0][2] == "garden"
    assert rows[0][3] == Decimal("0.00")  # a view has no revenue
    assert rows[0][4] == 1

    # updated_at is injected as a literal now() through execute_values'
    # custom template, not bound as a parameter -- checked explicitly because
    # a template that silently failed to substitute would leave this NULL and
    # violate the NOT NULL column rather than erroring anywhere obvious.
    with pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute(f"SELECT updated_at FROM {config.METRICS_TABLE}")
            assert cur.fetchone()[0] is not None


def test_write_metrics_batch_accumulates_deltas_instead_of_replacing(spark, pg_conn, clean_tables):
    """The DO UPDATE ... total_revenue + EXCLUDED.total_revenue guarantee this
    table exists for. Each batch contributes only ITS OWN delta -- the rows
    that batch actually inserted -- so a later batch landing in the same
    window must ADD to the stored totals, not replace them. A plain INSERT
    would duplicate the row; DO NOTHING (write_events_batch's rule) would
    freeze the window at whatever the first batch happened to see.
    """
    _write_full_batch(spark, [_purchase("10.00")], batch_id=0)
    rows = _metrics_rows(pg_conn)
    assert len(rows) == 1
    assert rows[0][3] == Decimal("10.00")
    assert rows[0][4] == 1

    # A later micro-batch lands in the SAME window. It knows nothing about
    # the first one -- it only carries its own 5.00 -- so the stored total
    # must become 15.00 by accumulation, in one row.
    _write_full_batch(spark, [_purchase("5.00")], batch_id=1)

    rows = _metrics_rows(pg_conn)
    assert len(rows) == 1, "the same window/category must accumulate into ONE row, not duplicate"
    assert rows[0][3] == Decimal("15.00"), "the second batch's delta must be ADDED to the stored total"
    assert rows[0][4] == 2


def test_replaying_a_committed_batch_does_not_double_count_metrics(spark, pg_conn, clean_tables):
    """The other half of the accumulate design, and the reason it is safe at
    all: because a batch's metrics delta is derived from write_events_batch's
    RETURNING rather than from the batch's input, a replay of an
    already-committed batch contributes an EMPTY delta. Accumulating that is
    a no-op. Without this property, `+ EXCLUDED` would double-count every
    replayed batch straight into the reported revenue.
    """
    events = [_purchase("10.00") for _ in range(3)]

    _write_full_batch(spark, events, batch_id=0)
    before = _metrics_rows(pg_conn)
    assert before[0][3] == Decimal("30.00")
    assert before[0][4] == 3

    inserted = _write_full_batch(spark, events, batch_id=0)  # the replay

    assert inserted == [], "every row already conflicts, so nothing is returned to aggregate"
    assert _metrics_rows(pg_conn) == before, "a replayed batch must not move the stored totals at all"
    assert _events_row_count(pg_conn) == 3


def test_events_and_metrics_roll_back_together_when_a_batch_crashes(spark, pg_conn, clean_tables):
    """The bug that motivated pg_transaction, proven closed.

    When these two writes committed in SEPARATE transactions, a crash in
    between was silently unrecoverable: `events` kept its rows, the replay
    found every one of them already present, RETURNING came back empty, and
    that batch's revenue never reached event_metrics -- an under-count with
    no error raised anywhere. The delta is derived from what the INSERT did,
    so once that INSERT commits alone, the delta is gone for good.

    Sharing one transaction makes the crash roll BOTH back, which puts the
    replay back on the fully-recoverable path. This test crashes exactly in
    that window and then replays, asserting both halves.
    """
    events = [_purchase("10.00") for _ in range(10)]
    valid, _ = split_valid_invalid(_process(_events_df(spark, events)))
    events_out = finalize_events(valid)

    with pytest.raises(RuntimeError):
        with pg_transaction() as cur:
            inserted = write_events_batch(events_out, batch_id=0, cur=cur)
            assert len(inserted) == 10, "the events write itself succeeded before the crash"
            raise RuntimeError("simulated crash between the events write and the metrics write")

    assert _events_row_count(pg_conn) == 0, "the events write must roll back with the crash, not survive it"
    assert _metrics_rows(pg_conn) == []

    # Spark replays the same batch_id. Nothing committed, so RETURNING yields
    # every row again and the metrics delta is re-derivable -- precisely the
    # property that separate transactions destroyed.
    _write_full_batch(spark, events, batch_id=0)

    assert _events_row_count(pg_conn) == 10
    rows = _metrics_rows(pg_conn)
    assert len(rows) == 1
    assert rows[0][3] == Decimal("100.00"), "no revenue lost to the crash"
    assert rows[0][4] == 10


def test_write_metrics_batch_with_zero_rows_is_a_no_op(spark, pg_conn, clean_tables):
    """A batch that is entirely quarantine (so aggregate_by_window's input is
    empty) must not error trying to write/upsert zero rows."""
    bad = make_event(Faker(), now=_FROZEN_NOW)
    bad["event_id"] = None  # missing_event_id -- guaranteed rejected

    _write_full_batch(spark, [bad], batch_id=0)  # must not raise

    assert _metrics_rows(pg_conn) == []
