"""Synthetic e-commerce event generation and atomic publish to the directory
Spark watches.

Two independent pieces on purpose:
  - make_event / make_malformed_event: pure functions, one event dict in or
    out, no I/O, no timing. Easy to unit test in isolation.
  - write_batch / run: the I/O and scheduling side -- writing a CSV and
    publishing it atomically, then looping on a schedule.
"""

import csv
import os
import random
import time
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from faker import Faker

from src import config, schema
from src.logger import get_logger

logger = get_logger("generator")

# Bounded catalogs, not a fresh id per event: a real product/customer base is
# finite and reused, and repeat ids are what make later aggregation (Phase 9's
# per-category totals, any "most active user" style analysis) meaningful
# instead of every row being a singleton nobody groups on.
_USER_ID_MAX = 5000
_PRODUCT_ID_MAX = 2000

# Realistic e-commerce funnel shape: most traffic is views, a minority reaches
# a cart, fewer still complete a purchase.
_EVENT_TYPE_WEIGHTS = OrderedDict([
    ("view", 0.65),
    ("add_to_cart", 0.25),
    ("purchase", 0.10),
])

# The four defects understand.md's Phase 4 commits to. Each corrupts exactly
# one field, so a quarantined row's rejection reason (Phase 6) is unambiguous.
DEFECT_TYPES = ("null_product_id", "negative_price", "unparseable_event_time", "zero_quantity")


def _iso(dt: datetime) -> str:
    """ISO-8601 UTC, millisecond precision, trailing 'Z' -- e.g.
    2026-08-09T14:03:11.482Z.

    Not dt.isoformat(): that emits '+00:00' and 6-digit microseconds. Spark's
    default timestamp cast parses a 'Z'-suffixed instant correctly, so this
    format is a deliberate choice, not just cosmetic.
    """
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def make_event(fake: Faker, *, now: datetime | None = None) -> dict:
    """One realistic, valid event as a dict keyed by schema.CSV_COLUMNS.

    Takes an already-seeded Faker instance rather than seeding one internally
    -- seeding must happen ONCE per run (or per test), not per call, or every
    event in a run would come out identical. `now` is injectable so tests
    never depend on wall-clock time.
    """
    generated_at = now or datetime.now(timezone.utc)
    # A few hundred ms to ~2s between "the click happened" and "the batch was
    # flushed" -- small enough to be unremarkable, large enough that event_time
    # is a genuinely earlier instant than generated_at, which is what makes
    # Phase 9's group-by-event-time (rather than processing time) meaningful.
    event_time = generated_at - timedelta(milliseconds=fake.random_int(min=0, max=2000))

    return {
        "event_id": fake.uuid4(),
        "event_time": _iso(event_time),
        "generated_at": _iso(generated_at),
        "user_id": f"user-{fake.random_int(min=1, max=_USER_ID_MAX):05d}",
        "product_id": f"prod-{fake.random_int(min=1, max=_PRODUCT_ID_MAX):05d}",
        "category": fake.random_element(elements=schema.CATEGORIES),
        "event_type": fake.random_element(elements=_EVENT_TYPE_WEIGHTS),
        "price": f"{fake.pydecimal(left_digits=3, right_digits=2, min_value=1, max_value=500):.2f}",
        "quantity": str(fake.random_int(min=1, max=5)),
    }


def make_malformed_event(event: dict, defect: str) -> dict:
    """Corrupt exactly one field of an already-valid event.

    Pure and deterministic: given the same (event, defect), always returns
    the same result. No randomness of its own -- the caller (generate_batch)
    decides *which* defect via its own rng, which keeps "how a defect looks"
    independently testable from "which rows get one".
    """
    event = dict(event)
    if defect == "null_product_id":
        event["product_id"] = ""
    elif defect == "negative_price":
        event["price"] = f"-{event['price']}"
    elif defect == "unparseable_event_time":
        event["event_time"] = "not-a-timestamp"
    elif defect == "zero_quantity":
        event["quantity"] = "0"
    else:
        raise ValueError(f"unknown defect type: {defect!r} (expected one of {DEFECT_TYPES})")
    return event


def generate_batch(
    fake: Faker, rng: random.Random, batch_size: int, bad_rate: float, *, now: datetime | None = None
) -> list[dict]:
    """batch_size valid events, each independently given a bad_rate chance of
    being corrupted by exactly one randomly-chosen defect.

    rng is deliberately separate from fake's own seeded stream: consuming rng
    calls for the "is this bad / which defect" decision never perturbs the
    sequence of realistic values fake would otherwise produce, so changing
    bad_rate alone never changes what a clean run's values would have looked
    like.
    """
    events = []
    for _ in range(batch_size):
        event = make_event(fake, now=now)
        if rng.random() < bad_rate:
            event = make_malformed_event(event, rng.choice(DEFECT_TYPES))
        events.append(event)
    return events


def write_batch(events: list[dict], staging_dir, incoming_dir) -> Path:
    """Write `events` as a complete CSV into staging_dir, then publish it into
    incoming_dir with a single atomic os.replace().

    Not a direct write into incoming_dir: Spark's file source discovers work
    by listing that directory every trigger, with no way to tell a
    half-written file from a finished one -- read mid-write, the file gets
    marked processed in the checkpoint and the rest of it is lost for good.
    Not shutil.move either: across filesystems it can degrade to
    copy-then-delete, losing atomicity -- which is exactly why staging_dir and
    incoming_dir must stay siblings under the same mount (see src/config.py).
    """
    staging_dir, incoming_dir = Path(staging_dir), Path(incoming_dir)

    # Timestamp prefix keeps `ls` chronological; the short uuid4 suffix
    # guarantees uniqueness even for two batches written in the same
    # microsecond. Never a leading '.' or '_' -- Spark's file source silently
    # skips those.
    filename = f"events_{datetime.now(timezone.utc):%Y%m%dT%H%M%S%f}_{uuid4().hex[:8]}.csv"
    staging_path = staging_dir / filename
    incoming_path = incoming_dir / filename

    # newline="" is required by the csv module (otherwise its own \r\n
    # handling doubles up with Python's universal-newline translation on
    # Windows, producing a blank line after every row).
    with open(staging_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=schema.CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(events)

    os.replace(staging_path, incoming_path)
    return incoming_path


def run(
    *,
    rate: float = 50.0,
    batch_size: int = 100,
    duration: float | None = None,
    bad_rate: float = 0.02,
    seed: int | None = None,
    staging_dir=None,
    incoming_dir=None,
) -> None:
    """Emit batches on a loop until `duration` seconds have passed (or
    forever, until Ctrl+C, if duration is None).

    rate and batch_size are the two independent knobs; the interval between
    files is derived (batch_size / rate) rather than accepted as a third,
    separately-specified parameter -- three knobs for two real degrees of
    freedom invites contradictory settings (a rate that doesn't match a
    separately-given batch_size and interval).
    """
    if rate <= 0:
        raise ValueError(f"rate must be > 0 events/sec, got {rate}")
    if batch_size <= 0:
        raise ValueError(f"batch_size must be > 0, got {batch_size}")

    staging_dir = staging_dir or config.STAGING_DIR
    incoming_dir = incoming_dir or config.INCOMING_DIR
    interval = batch_size / rate

    fake = Faker()
    if seed is not None:
        fake.seed_instance(seed)
    rng = random.Random(seed)

    logger.info(
        "generator starting | rate=%.1f events/s batch_size=%d interval=%.3fs bad_rate=%.1f%% "
        "duration=%s seed=%s -> %s",
        rate,
        batch_size,
        interval,
        bad_rate * 100,
        duration if duration is not None else "unbounded",
        seed if seed is not None else "none (real random data)",
        incoming_dir,
    )

    start = time.monotonic()
    batch_num = 0
    total_events = 0
    try:
        while duration is None or (time.monotonic() - start) < duration:
            batch_start = time.monotonic()
            events = generate_batch(fake, rng, batch_size, bad_rate)
            path = write_batch(events, staging_dir, incoming_dir)
            batch_num += 1
            total_events += len(events)
            logger.info("batch %d: %d events -> %s (total %d)", batch_num, len(events), path.name, total_events)
            time.sleep(max(0.0, interval - (time.monotonic() - batch_start)))
    except KeyboardInterrupt:
        logger.info("generator stopped (Ctrl+C) after %d batches, %d events", batch_num, total_events)
