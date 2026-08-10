"""Tests for src/generator.py.

Pure Python, no Spark and no Docker -- these exercise value generation, the
atomic-publish file mechanics, and the run() loop's own timing/orchestration,
all directly on the filesystem via tmp_path.
"""

import csv
import random
import threading
import time
from datetime import datetime, timezone

import pytest
from faker import Faker

from src import schema
from src.generator import (
    DEFECT_TYPES,
    generate_batch,
    make_event,
    make_malformed_event,
    run,
    write_batch,
)

_FROZEN_NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def _seeded_fake(seed: int) -> Faker:
    fake = Faker()
    fake.seed_instance(seed)
    return fake


# --- make_event ---

def test_make_event_has_exactly_the_schema_columns():
    event = make_event(_seeded_fake(1), now=_FROZEN_NOW)
    assert set(event.keys()) == set(schema.CSV_COLUMNS)


def test_make_event_is_deterministic_under_a_seed():
    e1 = make_event(_seeded_fake(42), now=_FROZEN_NOW)
    e2 = make_event(_seeded_fake(42), now=_FROZEN_NOW)
    assert e1 == e2


def test_make_event_differs_across_seeds():
    e1 = make_event(_seeded_fake(1), now=_FROZEN_NOW)
    e2 = make_event(_seeded_fake(2), now=_FROZEN_NOW)
    assert e1 != e2


def test_make_event_event_time_is_at_or_before_generated_at():
    # ISO-8601 'Z' timestamps of equal precision compare lexically exactly
    # like they compare chronologically.
    event = make_event(_seeded_fake(7), now=_FROZEN_NOW)
    assert event["event_time"] <= event["generated_at"]


def test_make_event_generated_at_matches_the_injected_now():
    event = make_event(_seeded_fake(7), now=_FROZEN_NOW)
    assert event["generated_at"] == "2026-01-01T12:00:00.000Z"


def test_make_event_event_type_is_one_of_the_known_types():
    fake = _seeded_fake(9)
    seen = {make_event(fake, now=_FROZEN_NOW)["event_type"] for _ in range(200)}
    assert seen <= set(schema.EVENT_TYPES)


def test_make_event_category_is_one_of_the_known_categories():
    fake = _seeded_fake(9)
    seen = {make_event(fake, now=_FROZEN_NOW)["category"] for _ in range(200)}
    assert seen <= set(schema.CATEGORIES)


def test_make_event_price_and_quantity_are_well_formed_positive_values():
    fake = _seeded_fake(11)
    for _ in range(50):
        event = make_event(fake, now=_FROZEN_NOW)
        assert float(event["price"]) > 0
        assert int(event["quantity"]) > 0


# --- make_malformed_event ---

def test_make_malformed_event_null_product_id_touches_only_that_field():
    base = make_event(_seeded_fake(3), now=_FROZEN_NOW)
    bad = make_malformed_event(base, "null_product_id")
    assert bad["product_id"] == ""
    assert {k: v for k, v in bad.items() if k != "product_id"} == {
        k: v for k, v in base.items() if k != "product_id"
    }


def test_make_malformed_event_negative_price():
    base = make_event(_seeded_fake(3), now=_FROZEN_NOW)
    bad = make_malformed_event(base, "negative_price")
    assert float(bad["price"]) < 0
    assert abs(float(bad["price"])) == float(base["price"])


def test_make_malformed_event_unparseable_event_time():
    base = make_event(_seeded_fake(3), now=_FROZEN_NOW)
    bad = make_malformed_event(base, "unparseable_event_time")
    with pytest.raises(ValueError):
        datetime.fromisoformat(bad["event_time"].replace("Z", "+00:00"))


def test_make_malformed_event_zero_quantity():
    base = make_event(_seeded_fake(3), now=_FROZEN_NOW)
    bad = make_malformed_event(base, "zero_quantity")
    assert bad["quantity"] == "0"


def test_make_malformed_event_is_pure_and_deterministic():
    base = make_event(_seeded_fake(3), now=_FROZEN_NOW)
    results = {make_malformed_event(base, "negative_price")["price"] for _ in range(10)}
    assert len(results) == 1


def test_make_malformed_event_rejects_an_unknown_defect():
    base = make_event(_seeded_fake(3), now=_FROZEN_NOW)
    with pytest.raises(ValueError):
        make_malformed_event(base, "not_a_real_defect")


# --- generate_batch ---

def test_generate_batch_is_deterministic_under_a_seed():
    fake1, rng1 = _seeded_fake(123), random.Random(123)
    fake2, rng2 = _seeded_fake(123), random.Random(123)
    batch1 = generate_batch(fake1, rng1, batch_size=200, bad_rate=0.1, now=_FROZEN_NOW)
    batch2 = generate_batch(fake2, rng2, batch_size=200, bad_rate=0.1, now=_FROZEN_NOW)
    assert batch1 == batch2


def _looks_malformed(event: dict) -> bool:
    """Mirrors the exact defects Phase 4 injects (and Phase 3's CHECK
    constraints reject), rather than reaching into generator internals --
    a black-box definition of "bad" that also cross-checks Phase 3's DDL
    actually matches what Phase 4 actually produces.
    """
    if event["product_id"] == "":
        return True
    if float(event["price"]) < 0:
        return True
    if int(event["quantity"]) == 0:
        return True
    try:
        datetime.fromisoformat(event["event_time"].replace("Z", "+00:00"))
    except ValueError:
        return True
    return False


def test_bad_row_fraction_is_within_tolerance_of_bad_rate():
    fake, rng = _seeded_fake(99), random.Random(99)
    bad_rate = 0.02
    batch = generate_batch(fake, rng, batch_size=5000, bad_rate=bad_rate, now=_FROZEN_NOW)
    fraction = sum(1 for e in batch if _looks_malformed(e)) / len(batch)
    # Binomial sample around a 2% true rate at n=5000 -- generous tolerance,
    # this is a sanity bound, not a statistical precision test.
    assert 0.01 <= fraction <= 0.035, f"bad-row fraction {fraction:.4f} outside tolerance for bad_rate={bad_rate}"


def test_zero_bad_rate_produces_no_malformed_rows():
    fake, rng = _seeded_fake(1), random.Random(1)
    batch = generate_batch(fake, rng, batch_size=500, bad_rate=0.0, now=_FROZEN_NOW)
    assert not any(_looks_malformed(e) for e in batch)


# --- write_batch ---

def test_write_batch_header_matches_csv_columns(tmp_path):
    staging, incoming = tmp_path / "staging", tmp_path / "incoming"
    staging.mkdir()
    incoming.mkdir()
    events = [make_event(_seeded_fake(i), now=_FROZEN_NOW) for i in range(5)]

    path = write_batch(events, staging, incoming)

    assert path.parent == incoming
    with open(path, newline="", encoding="utf-8") as f:
        header = next(csv.reader(f))
    assert header == schema.CSV_COLUMNS


def test_write_batch_leaves_staging_empty(tmp_path):
    staging, incoming = tmp_path / "staging", tmp_path / "incoming"
    staging.mkdir()
    incoming.mkdir()
    events = [make_event(_seeded_fake(i), now=_FROZEN_NOW) for i in range(5)]

    write_batch(events, staging, incoming)

    assert list(staging.iterdir()) == []


def test_write_batch_round_trips_every_row_exactly(tmp_path):
    staging, incoming = tmp_path / "staging", tmp_path / "incoming"
    staging.mkdir()
    incoming.mkdir()
    events = [make_event(_seeded_fake(i), now=_FROZEN_NOW) for i in range(10)]

    path = write_batch(events, staging, incoming)

    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert rows == events


def test_write_batch_filename_has_no_leading_dot_or_underscore(tmp_path):
    """Spark's file source silently skips hidden-style files -- a leading '.'
    or '_' would mean the file is written but never discovered, with no error
    anywhere. Confirmed here rather than only asserted in a comment."""
    staging, incoming = tmp_path / "staging", tmp_path / "incoming"
    staging.mkdir()
    incoming.mkdir()
    path = write_batch([make_event(_seeded_fake(1), now=_FROZEN_NOW)], staging, incoming)
    assert not path.name.startswith(".")
    assert not path.name.startswith("_")


def test_incoming_never_shows_a_partial_file_during_a_write(tmp_path):
    """Black-box confirmation of the atomic-publish guarantee: poll
    incoming/ continuously while a real write is in flight, and assert every
    file ever observed there is already complete. This can only pass by
    construction (write_batch never touches incoming_dir until the single
    os.replace() at the end) -- which is exactly the point: it would fail
    loudly if write_batch were ever "simplified" to write into incoming_dir
    directly.
    """
    staging, incoming = tmp_path / "staging", tmp_path / "incoming"
    staging.mkdir()
    incoming.mkdir()
    fake = _seeded_fake(5)
    events = [make_event(fake, now=_FROZEN_NOW) for _ in range(5000)]

    violations = []
    stop = threading.Event()

    def poll():
        while not stop.is_set():
            for f in incoming.iterdir():
                try:
                    with open(f, newline="", encoding="utf-8") as fh:
                        rows = list(csv.DictReader(fh))
                except FileNotFoundError:
                    continue  # rename raced the listdir; nothing to check
                if len(rows) != len(events):
                    violations.append(f"{f.name}: saw {len(rows)} rows, expected {len(events)}")
            time.sleep(0.001)

    poller = threading.Thread(target=poll, daemon=True)
    poller.start()
    write_batch(events, staging, incoming)
    time.sleep(0.05)
    stop.set()
    poller.join(timeout=2)

    assert violations == [], f"observed an incomplete file in incoming/: {violations}"


# --- run() ---

def test_run_respects_duration_and_writes_at_least_one_file(tmp_path):
    staging, incoming = tmp_path / "staging", tmp_path / "incoming"
    staging.mkdir()
    incoming.mkdir()

    started = time.monotonic()
    run(rate=200.0, batch_size=10, duration=0.15, bad_rate=0.0, seed=1, staging_dir=staging, incoming_dir=incoming)
    elapsed = time.monotonic() - started

    files = list(incoming.iterdir())
    assert len(files) >= 1
    assert list(staging.iterdir()) == []
    assert elapsed < 5.0  # duration is a floor checked between batches, not a preemptive cutoff


def test_run_rejects_a_non_positive_rate(tmp_path):
    with pytest.raises(ValueError):
        run(rate=0, duration=0.01, staging_dir=tmp_path, incoming_dir=tmp_path)


def test_run_rejects_a_non_positive_batch_size(tmp_path):
    with pytest.raises(ValueError):
        run(batch_size=0, duration=0.01, staging_dir=tmp_path, incoming_dir=tmp_path)
