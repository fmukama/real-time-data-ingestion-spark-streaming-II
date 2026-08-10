"""Turns Spark's own per-micro-batch progress reports into logs/metrics.jsonl
-- one JSON line per batch, which is what turns docs/performance_metrics.md
into analysis of a real, measured dataset instead of prose.
"""

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQueryListener

from src import config
from src.logger import get_logger

logger = get_logger("monitoring")

_listener_registered = False


class MetricsListener(StreamingQueryListener):
    """event.progress.json is already a complete, correctly-formatted record
    of one micro-batch -- confirmed directly against a real running query:
    batchId, numInputRows, inputRowsPerSecond, processedRowsPerSecond,
    batchDuration, the durationMs breakdown (addBatch/getBatch/latestOffset/...
    -- addBatch is normally where the time actually goes, i.e. the Postgres
    write), plus per-source/sink detail understand.md's own minimal field list
    doesn't even ask for. Writing that JSON directly, rather than hand-picking
    a smaller dict from the same fields, means there is nothing here to
    transcribe wrong.
    """

    def onQueryStarted(self, event) -> None:
        logger.info("streaming query started | id=%s name=%s", event.id, event.name)

    def onQueryProgress(self, event) -> None:
        with open(config.METRICS_FILE, "a", encoding="utf-8") as f:
            f.write(event.progress.json + "\n")

    def onQueryIdle(self, event) -> None:
        pass  # not a batch -- metrics.jsonl is specifically one line per batch

    def onQueryTerminated(self, event) -> None:
        if event.exception:
            logger.error("streaming query terminated with an error: %s", event.exception)
        else:
            logger.info("streaming query terminated cleanly")


def register(spark: SparkSession) -> None:
    """Registers ONE MetricsListener on the session.

    Guarded the same way src/logger.py guards against duplicate handlers:
    spark.streams.addListener(...) has no built-in "already registered" check
    of its own, and calling this twice would silently double (or triple, ...)
    every metrics line -- a real risk once a shared SparkSession is reused
    across multiple calls (e.g. a session-scoped test fixture), not just a
    defensive-programming exercise for a scenario that can't happen.
    """
    global _listener_registered
    if _listener_registered:
        return
    spark.streams.addListener(MetricsListener())
    _listener_registered = True
    logger.info("metrics listener registered -> %s", config.METRICS_FILE)
