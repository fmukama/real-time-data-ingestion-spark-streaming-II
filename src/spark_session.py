"""SparkSession construction for the streaming job, the generator's tests, and
the test suite.

Relies on `SparkSession.builder.getOrCreate()`'s own JVM-level idempotency
rather than a hand-rolled singleton: calling this twice in one process returns
the same session, which is what makes a session-scoped pytest fixture safe.
"""

from pyspark.sql import SparkSession

from src import config
from src.logger import get_logger

logger = get_logger("spark_session")


def get_spark_session(app_name: str | None = None) -> SparkSession:
    """Build (or return) the project's SparkSession.

    Every option set here is deliberate; see the inline notes for why.
    """
    app_name = app_name or config.SPARK_APP_NAME

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(config.SPARK_MASTER)

        # The PostgreSQL JDBC driver, baked into the image at build time. Set as
        # spark.jars (a local path) rather than spark.jars.packages (a Maven
        # coordinate) so nothing is resolved over the network at startup.
        .config("spark.jars", config.JDBC_JAR)

        # See config.SHUFFLE_PARTITIONS: the 200 default is both wasteful at this
        # data size and a genuine hazard, because the JDBC writer opens one
        # connection per partition.
        .config("spark.sql.shuffle.partitions", config.SHUFFLE_PARTITIONS)

        # Pin the session timezone so timestamp parsing and windowing never
        # depend on the container's locale. See config.SPARK_TIMEZONE.
        .config("spark.sql.session.timeZone", config.SPARK_TIMEZONE)

        # Explicit rather than implicit: schema inference on a streaming file
        # source is off by default, and it must stay off. Inference requires
        # Spark to peek at files to guess types, and the guess can differ
        # between micro-batches -- a column that changes type mid-stream.
        # src/schema.py owns the schema instead (Phase 3).
        .config("spark.sql.streaming.schemaInference", "false")

        # Must be set HERE, at build time -- confirmed directly that this is a
        # "static" config; setting it later via spark.conf.set() on an
        # already-created session has no effect. See config.FILE_SOURCE_CLEANUP_DELAY
        # for what this fixes (a 10-minute default that hides cleanSource=archive
        # from any reasonably fast demo or test).
        .config("spark.sql.streaming.fileSource.log.cleanupDelay", config.FILE_SOURCE_CLEANUP_DELAY)

        .getOrCreate()
    )

    # Spark's own INFO chatter would bury the pipeline's log lines; WARN keeps
    # the genuinely useful messages (and any Spark warning we should see).
    spark.sparkContext.setLogLevel("WARN")

    logger.info(
        "SparkSession ready | app=%s master=%s spark=%s shuffle_partitions=%s tz=%s",
        app_name,
        config.SPARK_MASTER,
        spark.version,
        config.SHUFFLE_PARTITIONS,
        config.SPARK_TIMEZONE,
    )
    return spark


def stop_spark_session(spark: SparkSession) -> None:
    """Stop a session and say so. Worth a function of its own because a
    long-running streaming job that exits without stopping its session leaves
    the JVM alive and the Spark UI port held."""
    spark.stop()
    logger.info("SparkSession stopped.")
