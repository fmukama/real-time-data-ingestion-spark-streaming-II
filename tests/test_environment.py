"""Environment wiring checks.

These exist because the sibling project shipped a `make test` that was broken
for the entire build and nobody noticed until the final phase: the Makefile
called bare `pytest`, which (unlike `python -m pytest`) does not prepend the CWD
to sys.path, so `from src import ...` never resolved. The fix is pytest.ini's
`pythonpath = .` -- and this file is what proves it still works.

Deliberately fast: no SparkSession is built here. These assertions are about the
container and the import path, not about Spark behaviour, and they should stay
runnable in well under a second so there is no reason to skip them.
"""

import os

import pytest


def test_src_package_is_importable():
    """The bug this file exists for. If pytest.ini's pythonpath is lost or the
    Makefile invocation changes, this fails immediately instead of at Phase 10."""
    from src import config, logger, spark_session  # noqa: F401


def test_pyspark_is_importable_and_is_the_expected_major():
    """Guards the Dockerfile's PYTHONPATH/py4j-symlink work. A `docker compose
    exec` session bypasses the image's before-notebook.d hook, so this import
    only succeeds because SPARK_HOME/PYTHONPATH are baked in as ENV."""
    import pyspark

    assert pyspark.__version__.startswith("4."), (
        f"expected Spark 4.x from the pinned base image, got {pyspark.__version__}"
    )


def test_jdbc_driver_jar_is_present():
    """The jar is baked in at build time rather than resolved from Maven at run
    time, so its absence is a build problem to catch here -- not a mysterious
    ClassNotFoundException in the middle of a streaming batch."""
    from src import config

    assert os.path.isfile(config.JDBC_JAR), f"JDBC jar missing at {config.JDBC_JAR}"
    assert os.path.getsize(config.JDBC_JAR) > 0, "JDBC jar is empty"


def test_data_directories_exist_and_are_writable():
    """staging -> incoming is the atomic-publish path (understand.md Phase 4);
    both must exist and be writable before the generator can work."""
    from src import config

    for path in (config.STAGING_DIR, config.INCOMING_DIR, config.ARCHIVE_DIR):
        assert os.path.isdir(path), f"missing directory: {path}"
        assert os.access(path, os.W_OK), f"not writable: {path}"


def test_staging_and_incoming_share_a_filesystem():
    """os.replace() is only atomic within one filesystem. If these ever diverge,
    the rename silently degrades to copy-then-delete and Spark can once again
    read a half-written file -- the exact failure the design is built to avoid.

    st_dev is the filesystem identity, so comparing it is the real check rather
    than assuming two sibling paths must be co-located.
    """
    from src import config

    assert (
        os.stat(config.STAGING_DIR).st_dev == os.stat(config.INCOMING_DIR).st_dev
    ), "staging/ and incoming/ are on different filesystems; os.replace() would not be atomic"


def test_jdbc_url_uses_the_service_name_not_localhost():
    """Inside a container `localhost` is that container, not the database. This
    is the single most common setup mistake in this stack."""
    from src import config

    if os.path.exists("/.dockerenv"):
        assert "localhost" not in config.JDBC_URL, (
            "JDBC_URL points at localhost from inside a container; it must use the "
            f"compose service name. Got: {config.JDBC_URL}"
        )
    else:
        pytest.skip("not running inside a container; host-side localhost is correct")


def test_jdbc_url_enables_rewrite_batched_inserts():
    """One URL parameter, frequently a several-fold insert throughput win. Easy
    to lose in a refactor and invisible when it goes missing."""
    from src import config

    assert "reWriteBatchedInserts=true" in config.JDBC_URL
