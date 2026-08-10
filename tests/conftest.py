"""Shared fixtures across the whole test suite.

spark: session-scoped so every test file shares ONE SparkSession rather than
each starting its own -- get_spark_session()'s own getOrCreate() idempotency
already made repeated per-file fixtures harmless, but a single shared fixture
is simpler and marginally faster than each test module tearing down and
rebuilding.

pg_conn / clean_tables: only needed by @pytest.mark.integration tests (the
ones in tests/test_sink_idempotency.py), which are the only tests in this
suite that talk to a live Postgres. Everything else never touches these.
"""

import psycopg2
import pytest

from src import config
from src.spark_session import get_spark_session, stop_spark_session


@pytest.fixture(scope="session")
def spark():
    session = get_spark_session("test-suite")
    yield session
    stop_spark_session(session)


@pytest.fixture
def pg_conn():
    """A fresh psycopg2 connection per test, closed afterward regardless of
    outcome -- a test that fails partway through a transaction must not leak
    a connection into the next test.
    """
    conn = psycopg2.connect(**config.pg_connect_kwargs())
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture
def clean_tables(pg_conn):
    """Truncates events/events_quarantine/event_metrics before AND after the
    test, so an integration test never sees another test's leftover rows and
    never leaves its own behind for the next one. Any staging_events_*/
    staging_metrics_* table left by a crashed run partway through a test is
    also swept up -- the same "disposable, always safe to drop" property
    write_events_batch/write_metrics_batch themselves rely on.

    event_metrics was added in Phase 9, after this fixture already existed --
    caught directly, the hard way, by a real test failure: without it here,
    a row test_write_metrics_batch_upserts_replacing_not_duplicating wrote
    leaked straight into test_write_metrics_batch_with_zero_rows_is_a_no_op,
    which then failed on what looked like an unrelated assertion. The fixture
    covering every table a sink actually writes to isn't optional cleanup --
    it's what makes each integration test's result belong to that test alone.
    """

    def _clean():
        with pg_conn:
            with pg_conn.cursor() as cur:
                cur.execute(f"TRUNCATE {config.EVENTS_TABLE}, {config.QUARANTINE_TABLE}, {config.METRICS_TABLE}")
                cur.execute(
                    "SELECT tablename FROM pg_tables WHERE schemaname = 'public' "
                    "AND (tablename LIKE 'staging_events_%' OR tablename LIKE 'staging_metrics_%')"
                )
                for (table_name,) in cur.fetchall():
                    cur.execute(f"DROP TABLE IF EXISTS {table_name}")

    _clean()
    yield
    _clean()
