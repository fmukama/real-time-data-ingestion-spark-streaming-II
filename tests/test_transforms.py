"""Tests for src/transforms.py.

Every transform is DataFrame -> DataFrame with no I/O, so every test here
builds a small in-memory batch DataFrame directly -- no streaming, no
Docker-Postgres, all green in well under a second per test. That is the
entire payoff of the transform pipeline's pure-function design.
"""

import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from faker import Faker
from pyspark.sql import functions as F

from src import schema
from src.generator import generate_batch, make_event, make_malformed_event
from src.schema import CORRUPT_RECORD_COLUMN
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

_FROZEN_NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

# Must match sql/postgres_setup.sql's column sets exactly. A SEPARATE,
# independent check (tests/test_schema_contract.py) already guards that file
# against drift from src/schema.py; this list exists to check finalize_events/
# finalize_quarantine's OWN correctness, not to re-verify the SQL file itself.
_EXPECTED_EVENTS_COLUMNS = [
    "event_id", "event_time", "generated_at", "user_id", "product_id",
    "category", "event_type", "price", "quantity", "revenue",
    "ingested_at", "latency_ms",
]
_EXPECTED_QUARANTINE_COLUMNS = [
    "event_id", "event_time", "generated_at", "user_id", "product_id",
    "category", "event_type", "price", "quantity", "rejection_reason",
    "raw_line", "quarantined_at",
]


# spark fixture: tests/conftest.py (session-scoped, shared across the suite)


def _events_df(spark, rows):
    """rows: list[dict] shaped like schema.CSV_COLUMNS (+ optionally
    _corrupt_record). Builds a DataFrame the same shape build_source produces:
    every column a string, since that is what the reader always hands
    downstream code -- cast_types is what turns it into real types.

    Converts "" to None for every field, matching the real CSV reader's
    default nullValue="" (confirmed directly): make_malformed_event
    writes "" for its null_product_id defect, and going straight from a Python
    tuple to a DataFrame -- unlike a real run, which always round-trips through
    an actual CSV file -- would otherwise skip that conversion and leave an
    empty string where production would have a genuine NULL, silently making
    the fixture unfaithful to what the pipeline actually receives.
    """
    columns = schema.CSV_COLUMNS + [CORRUPT_RECORD_COLUMN]
    tuples = [tuple((row.get(c) or None) for c in columns) for row in rows]
    return spark.createDataFrame(tuples, ", ".join(f"{c} string" for c in columns))


def _process(df):
    """The pipeline in its documented order: cast,
    normalise, derive, then add the rejection reason."""
    df = cast_types(df)
    df = normalise(df)
    df = derive(df)
    df = add_rejection_reason(df)
    return df


def _one_event(**overrides):
    fake = Faker()
    fake.seed_instance(1)
    event = make_event(fake, now=_FROZEN_NOW)
    event.update(overrides)
    return event


# --- cast_types ---

def test_cast_types_casts_all_four_typed_columns(spark):
    df = cast_types(_events_df(spark, [_one_event()]))
    row = df.collect()[0]
    assert row["price_num"] is not None
    assert isinstance(row["quantity_num"], int)
    assert row["event_time_ts"] is not None
    assert row["generated_at_ts"] is not None


def test_cast_types_leaves_original_string_columns_untouched(spark):
    event = _one_event()
    df = cast_types(_events_df(spark, [event]))
    row = df.collect()[0]
    assert row["price"] == event["price"]
    assert row["quantity"] == event["quantity"]
    assert isinstance(row["price"], str)


def test_cast_types_unparseable_timestamp_yields_null_not_an_error(spark):
    bad = make_malformed_event(_one_event(), "unparseable_event_time")
    df = cast_types(_events_df(spark, [bad]))
    row = df.collect()[0]
    assert row["event_time_ts"] is None
    assert row["event_time"] == "not-a-timestamp"


# --- normalise ---

def test_normalise_trims_and_lowercases(spark):
    event = _one_event(event_type="  Purchase  ", category="  Books ")
    row = normalise(_events_df(spark, [event])).collect()[0]
    assert row["event_type"] == "purchase"
    assert row["category"] == "books"


def test_normalise_preserves_null_category(spark):
    event = _one_event(category=None)
    row = normalise(_events_df(spark, [event])).collect()[0]
    assert row["category"] is None


# --- derive ---

def test_derive_computes_revenue_for_purchase_only(spark):
    purchase = _one_event(event_type="purchase", price="10.00", quantity="3")
    view = _one_event(event_type="view", price="10.00", quantity="3")
    df = derive(normalise(cast_types(_events_df(spark, [purchase, view]))))
    rows = {r["event_type"]: r for r in df.collect()}
    assert rows["purchase"]["revenue"] == Decimal("30.00")
    assert rows["view"]["revenue"] is None


def test_derive_latency_ms_reflects_the_real_gap(spark):
    real_generated_at = datetime.now(timezone.utc) - timedelta(seconds=5)
    event = make_event(Faker(), now=real_generated_at)
    df = derive(normalise(cast_types(_events_df(spark, [event]))))
    latency = df.collect()[0]["latency_ms"]
    # ingested_at is a real current_timestamp() at execution time; generated_at
    # was pinned 5s in the past. A generous window (not just "non-negative")
    # so this is a real check of the subtraction, not just "didn't crash".
    assert 4000 <= latency <= 30000


def test_derive_does_not_crash_when_generated_at_failed_to_parse(spark):
    bad = make_malformed_event(_one_event(), "unparseable_event_time")
    # event_time is what this defect corrupts; also exercise generated_at
    # being unparseable, which cast_types leaves NULL the same way.
    bad["generated_at"] = "not-a-timestamp"
    df = derive(normalise(cast_types(_events_df(spark, [bad]))))
    assert df.collect()[0]["latency_ms"] is None  # NULL propagates, no crash


# --- add_rejection_reason / split_valid_invalid ---

def test_all_valid_batch_yields_zero_rejects(spark):
    fake = Faker()
    fake.seed_instance(1)
    events = [make_event(fake, now=_FROZEN_NOW) for _ in range(10)]
    df = _process(_events_df(spark, events))
    valid, invalid = split_valid_invalid(df)
    assert valid.count() == 10
    assert invalid.count() == 0


@pytest.mark.parametrize(
    "defect,expected_reason",
    [
        ("null_product_id", "missing_product_id"),
        ("negative_price", "invalid_price"),
        ("unparseable_event_time", "unparseable_event_time"),
        ("zero_quantity", "invalid_quantity"),
    ],
)
def test_each_generator_defect_yields_its_expected_rejection_reason(spark, defect, expected_reason):
    """Closes the loop between the generator's defects and the transform rules: fails if
    the generator's defect names and these rules ever drift apart in meaning,
    not just in name."""
    bad = make_malformed_event(_one_event(), defect)
    df = _process(_events_df(spark, [bad]))
    assert df.collect()[0]["rejection_reason"] == expected_reason


def test_structurally_corrupt_line_is_caught_first_even_with_other_bad_fields(spark):
    event = _one_event(price="-999.00")  # would ALSO trigger invalid_price
    event[CORRUPT_RECORD_COLUMN] = "some,raw,corrupt,line"
    df = _process(_events_df(spark, [event]))
    assert df.collect()[0]["rejection_reason"] == "structurally_corrupt_line"


def test_missing_event_id_is_caught(spark):
    event = _one_event(event_id=None)
    df = _process(_events_df(spark, [event]))
    assert df.collect()[0]["rejection_reason"] == "missing_event_id"


def test_missing_user_id_is_caught(spark):
    event = _one_event(user_id=None)
    df = _process(_events_df(spark, [event]))
    assert df.collect()[0]["rejection_reason"] == "missing_user_id"


def test_null_event_type_is_caught_not_silently_treated_as_valid(spark):
    """Regression test for a real bug caught while designing this phase:
    ~col.isin(...) on a NULL column evaluates to NULL, not true, and a NULL
    when() condition does not fire -- so a naive "not a known type" check
    alone would let a genuinely null event_type through as if it were valid.
    Confirmed the failure mode directly before writing the fix; this test is
    what stops it from being silently reintroduced.
    """
    event = _one_event(event_type=None)
    df = _process(_events_df(spark, [event]))
    assert df.collect()[0]["rejection_reason"] == "unknown_event_type"


def test_unrecognised_event_type_is_caught(spark):
    event = _one_event(event_type="refund")
    df = _process(_events_df(spark, [event]))
    assert df.collect()[0]["rejection_reason"] == "unknown_event_type"


def test_unparseable_generated_at_is_caught(spark):
    event = _one_event(generated_at="not-a-timestamp")
    df = _process(_events_df(spark, [event]))
    assert df.collect()[0]["rejection_reason"] == "unparseable_generated_at"


# --- finalize_events / finalize_quarantine ---

def test_finalize_events_produces_exactly_the_events_table_columns(spark):
    df = _process(_events_df(spark, [_one_event()]))
    valid, _ = split_valid_invalid(df)
    assert finalize_events(valid).columns == _EXPECTED_EVENTS_COLUMNS


def test_finalize_quarantine_produces_exactly_the_quarantine_table_columns(spark):
    bad = make_malformed_event(_one_event(), "zero_quantity")
    df = _process(_events_df(spark, [bad]))
    _, invalid = split_valid_invalid(df)
    assert finalize_quarantine(invalid).columns == _EXPECTED_QUARANTINE_COLUMNS


def test_finalize_events_types_match_postgres_column_types(spark):
    df = _process(_events_df(spark, [_one_event()]))
    valid, _ = split_valid_invalid(df)
    types = dict(finalize_events(valid).dtypes)
    assert types["price"].startswith("decimal(10,2)")
    assert types["quantity"] == "int"
    assert types["event_time"] == "timestamp"
    assert types["generated_at"] == "timestamp"


def test_finalize_quarantine_keeps_the_original_string_value_not_the_typed_one(spark):
    """The whole point of the quarantine table: preserve what was actually
    received, even -- especially -- when that's the reason it was rejected."""
    bad = make_malformed_event(_one_event(), "negative_price")
    df = _process(_events_df(spark, [bad]))
    _, invalid = split_valid_invalid(df)
    row = finalize_quarantine(invalid).collect()[0]
    assert row["price"] == bad["price"]
    assert isinstance(row["price"], str)


def test_finalize_quarantine_captures_raw_line_for_a_corrupt_record(spark):
    event = _one_event()
    event[CORRUPT_RECORD_COLUMN] = "raw,broken,line"
    df = _process(_events_df(spark, [event]))
    _, invalid = split_valid_invalid(df)
    assert finalize_quarantine(invalid).collect()[0]["raw_line"] == "raw,broken,line"


# --- End to end ---

def test_end_to_end_pipeline_on_a_realistic_mixed_batch(spark):
    """Uses the real generator's generate_batch, not hand-built fixtures, so
    this fails if the generator's defects and the transform rules ever drift apart."""
    fake, rng = Faker(), random.Random(77)
    fake.seed_instance(77)
    events = generate_batch(fake, rng, batch_size=200, bad_rate=0.25, now=_FROZEN_NOW)
    for e in events:
        e.setdefault(CORRUPT_RECORD_COLUMN, None)

    df = _process(_events_df(spark, events))
    valid, invalid = split_valid_invalid(df)
    events_out = finalize_events(valid)
    quarantine_out = finalize_quarantine(invalid)

    assert events_out.count() + quarantine_out.count() == len(events)
    assert quarantine_out.count() > 0  # bad_rate=0.25 guarantees some rejects
    assert events_out.count() > 0
    assert events_out.filter(F.col("price") < 0).count() == 0
    assert events_out.filter(F.col("quantity") < 1).count() == 0


# --- aggregate_by_window (stretch goal) ---
#
# aggregate_by_window takes list[dict], not a DataFrame -- the one deliberate
# exception to this module's DataFrame -> DataFrame rule (see the function's
# own docstring for the full story of why). Its input shape is exactly
# write_events_batch's RETURNING clause: real Python datetime/Decimal values
# for the rows a batch actually inserted, not a Spark Row and not the
# CSV-string shape _one_event produces above. So these tests build that shape
# directly -- no Spark session needed for any test in this block.

def _inserted_row(event_time, category, revenue):
    """A dict shaped exactly like one row of write_events_batch's RETURNING
    (event_time, category, revenue) -- aggregate_by_window's real input."""
    return {
        "event_time": datetime.fromisoformat(event_time.replace("Z", "+00:00")),
        "category": category,
        "revenue": revenue,
    }


def test_aggregate_by_window_groups_by_minute_and_category():
    purchase1 = _inserted_row("2026-01-01T00:00:10.000Z", "books", Decimal("20.00"))  # 10*2
    purchase2 = _inserted_row("2026-01-01T00:00:20.000Z", "books", Decimal("5.00"))  # 5*1
    view = _inserted_row("2026-01-01T00:00:15.000Z", "toys", None)  # view: no revenue

    metrics = {row["category"]: row for row in aggregate_by_window([purchase1, purchase2, view])}

    assert metrics["books"]["total_revenue"] == Decimal("25.00")  # 10*2 + 5*1
    assert metrics["books"]["event_count"] == 2
    assert metrics["toys"]["total_revenue"] == Decimal("0.00")  # view has no revenue
    assert metrics["toys"]["event_count"] == 1


def test_aggregate_by_window_separates_events_into_different_minute_windows():
    first_minute = _inserted_row("2026-01-01T00:00:30.000Z", "books", Decimal("10.00"))
    second_minute = _inserted_row("2026-01-01T00:01:30.000Z", "books", Decimal("20.00"))

    rows = sorted(aggregate_by_window([first_minute, second_minute]), key=lambda r: r["window_start"])

    assert len(rows) == 2
    assert rows[0]["total_revenue"] == Decimal("10.00")
    assert rows[1]["total_revenue"] == Decimal("20.00")
    # Adjacent, non-overlapping 1-minute windows, exactly 60 seconds apart.
    assert (rows[1]["window_start"] - rows[0]["window_start"]).total_seconds() == 60


def test_aggregate_by_window_output_shape_matches_event_metrics_columns():
    row = _inserted_row("2026-01-01T00:00:00.000Z", "books", Decimal("10.00"))
    metrics = aggregate_by_window([row])

    assert len(metrics) == 1
    assert set(metrics[0].keys()) == {
        "window_start", "window_end", "category", "total_revenue", "event_count",
    }
    assert isinstance(metrics[0]["total_revenue"], Decimal)
    assert metrics[0]["window_end"] - metrics[0]["window_start"] == timedelta(minutes=1)


def test_aggregate_by_window_zero_purchase_window_sums_to_zero_not_null():
    """A category with only non-purchase (revenue=None) events must sum to
    Decimal("0.00"), not None -- event_metrics.total_revenue is NOT NULL and
    the upsert would fail outright otherwise."""
    view = _inserted_row("2026-01-01T00:00:00.000Z", "garden", None)
    metrics = aggregate_by_window([view])

    assert metrics[0]["total_revenue"] == Decimal("0.00")
    assert metrics[0]["total_revenue"] is not None
    assert metrics[0]["event_count"] == 1


def test_aggregate_by_window_empty_input_returns_empty_list():
    """A batch write_events_batch inserted nothing for (every row a replay, or
    the whole batch quarantine) must produce zero metric rows, not error --
    process_batch calls aggregate_by_window unconditionally every batch."""
    assert aggregate_by_window([]) == []
