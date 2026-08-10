"""Guards the event contract (src/schema.py) against the specific failure mode
that motivates it: Spark's CSV reader matches an explicit schema against the
header BY POSITION, not by name, so if the generator's column order and
EVENT_SCHEMA's field order ever drifted, the result would be all-NULL rows
with no error anywhere.

Pure Python, no Spark and no Docker needed -- these run in milliseconds and
should never be skipped.
"""

import os
import re

import pytest

from src import config, schema


def test_csv_columns_matches_event_schema_field_order():
    """CSV_COLUMNS is *derived* from EVENT_SCHEMA (see schema.py), so this is
    a construction invariant today -- but that's the point. It stays true only
    because nothing ever hand-edits CSV_COLUMNS independently, and this test
    is what would catch the day someone does."""
    assert schema.CSV_COLUMNS == [f.name for f in schema.EVENT_SCHEMA.fields]


def test_event_schema_has_no_duplicate_field_names():
    names = [f.name for f in schema.EVENT_SCHEMA.fields]
    assert len(names) == len(set(names)), f"duplicate field names in EVENT_SCHEMA: {names}"


def test_every_event_schema_field_is_string_type():
    """Locks the deliberate all-string reader schema (module docstring in
    schema.py). Casting to real types happens explicitly in transforms.py,
    where a failed cast can be routed to quarantine with a reason -- a typed
    reader schema would instead turn a bad value into a silent NULL."""
    from pyspark.sql.types import StringType

    non_string = [f.name for f in schema.EVENT_SCHEMA.fields if not isinstance(f.dataType, StringType)]
    assert not non_string, f"expected every field StringType, found non-string: {non_string}"


def test_required_fields_are_a_subset_of_the_schema():
    """Catches a typo in REQUIRED_FIELDS (e.g. 'produt_id') that would
    otherwise silently never match anything in transforms.add_rejection_reason."""
    schema_fields = {f.name for f in schema.EVENT_SCHEMA.fields}
    unknown = set(schema.REQUIRED_FIELDS) - schema_fields
    assert not unknown, f"REQUIRED_FIELDS names not present in EVENT_SCHEMA: {unknown}"


def test_event_types_and_categories_are_nonempty_and_distinct():
    assert len(schema.EVENT_TYPES) == len(set(schema.EVENT_TYPES)) > 0
    assert len(schema.CATEGORIES) == len(set(schema.CATEGORIES)) > 0


# --- The sql/postgres_setup.sql side of the contract ---

_TABLE_CONSTRAINT_KEYWORDS = ("PRIMARY", "CONSTRAINT", "UNIQUE", "CHECK", "FOREIGN")


def _extract_table_columns(sql_text: str, table_name: str) -> list[str]:
    """Extract column names from a `CREATE TABLE <table_name> ( ... );` block.

    Deliberately simple (no SQL parser) rather than a false sense of generality:
    it relies on postgres_setup.sql's own consistent formatting -- one column
    definition per line, a closing `);` alone on its own line -- which is a file
    this project fully controls. See the header comment in that file.
    """
    pattern = re.compile(
        rf"CREATE TABLE\s+{re.escape(table_name)}\s*\((.*?)^\);",
        re.IGNORECASE | re.DOTALL | re.MULTILINE,
    )
    match = pattern.search(sql_text)
    assert match, f"could not find a `CREATE TABLE {table_name} ( ... );` block"

    columns = []
    for line in match.group(1).splitlines():
        line = line.strip().rstrip(",")
        if not line or line.startswith("--"):
            continue
        first_token = line.split()[0]
        if first_token.upper() in _TABLE_CONSTRAINT_KEYWORDS:
            continue
        columns.append(first_token)
    return columns


@pytest.fixture(scope="module")
def sql_text() -> str:
    path = os.path.join(config.BASE_DIR, "sql", "postgres_setup.sql")
    assert os.path.isfile(path), f"missing {path}"
    with open(path, encoding="utf-8") as f:
        return f.read()


def test_events_table_covers_every_schema_field(sql_text):
    """The other half of the contract: schema.py and postgres_setup.sql are two
    independently-edited files, and this is what stops them drifting apart."""
    events_columns = set(_extract_table_columns(sql_text, "events"))
    schema_fields = {f.name for f in schema.EVENT_SCHEMA.fields}
    missing = schema_fields - events_columns
    assert not missing, f"events table is missing schema field(s): {missing}"


def test_events_quarantine_table_covers_every_schema_field_plus_reason(sql_text):
    quarantine_columns = set(_extract_table_columns(sql_text, "events_quarantine"))
    schema_fields = {f.name for f in schema.EVENT_SCHEMA.fields}
    missing = schema_fields - quarantine_columns
    assert not missing, f"events_quarantine table is missing schema field(s): {missing}"
    assert "rejection_reason" in quarantine_columns


def test_event_metrics_table_has_the_composite_key_columns(sql_text):
    metrics_columns = set(_extract_table_columns(sql_text, "event_metrics"))
    assert {"window_start", "window_end", "category"} <= metrics_columns
