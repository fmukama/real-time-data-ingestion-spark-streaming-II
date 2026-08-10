"""Project-wide configuration: filesystem paths, PostgreSQL connection details,
and the streaming knobs the Phase 8 load matrix varies.

Everything tunable is read from the environment with a sensible default, so the
performance runs can change trigger interval or batch size via .env without
editing code -- and so the same module works inside the container and in a test.
"""

import os

from dotenv import load_dotenv

load_dotenv()

# --- Filesystem paths ---

BASE_DIR = os.path.dirname(os.path.dirname(__file__))

DATA_DIR = os.path.join(BASE_DIR, "data")

# The three-directory dance that makes file-based streaming safe (understand.md
# Phase 4). The generator writes a complete file into STAGING_DIR, then
# os.replace()s it into INCOMING_DIR -- an atomic rename, so Spark never sees a
# partial file. Spark watches INCOMING_DIR only, and moves consumed files to
# ARCHIVE_DIR so directory listing stays cheap.
#
# STAGING_DIR and INCOMING_DIR MUST stay on the same filesystem, or the rename
# degrades into copy-then-delete and stops being atomic. Keeping them siblings
# under data/ is what guarantees that.
STAGING_DIR = os.path.join(DATA_DIR, "staging")
INCOMING_DIR = os.path.join(DATA_DIR, "incoming")
ARCHIVE_DIR = os.path.join(DATA_DIR, "archive")

LOG_DIR = os.path.join(BASE_DIR, "logs")
LOG_FILE = os.path.join(LOG_DIR, "pipeline.log")
METRICS_FILE = os.path.join(LOG_DIR, "metrics.jsonl")

for _d in (STAGING_DIR, INCOMING_DIR, ARCHIVE_DIR, LOG_DIR):
    os.makedirs(_d, exist_ok=True)

# Checkpoints live in a named Docker volume mounted at /opt/checkpoints, NOT in
# the bind-mounted working tree -- Structured Streaming's checkpointing relies on
# atomic-rename semantics that Windows bind mounts don't reliably honour.
#
# Deliberately NOT created with os.makedirs here: on a non-container run this
# absolute path would resolve against the current drive root and litter the
# filesystem. The volume provides it, and Spark creates the sub-path itself.
CHECKPOINT_ROOT = os.getenv("CHECKPOINT_ROOT", "/opt/checkpoints")
EVENTS_CHECKPOINT = os.path.join(CHECKPOINT_ROOT, "events")

# --- PostgreSQL ---

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "ecommerce")
PG_USER = os.getenv("POSTGRES_USER", "streaming")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")

# reWriteBatchedInserts is a pgjdbc feature that collapses a batch of single-row
# INSERTs into multi-row INSERT statements. It is off by default and is one of
# the cheapest throughput wins available to this pipeline -- frequently several
# fold on insert-heavy workloads, for one URL parameter.
JDBC_URL = (
    f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}?reWriteBatchedInserts=true"
)

JDBC_DRIVER = "org.postgresql.Driver"

# Baked into the image at build time (see Dockerfile) rather than resolved from
# Maven on every run.
JDBC_JAR = os.getenv("POSTGRES_JDBC_JAR", "/opt/jars/postgresql-42.7.13.jar")

# How many rows the driver sends per round trip. Spark reads this out of the
# same properties map it uses for connection properties.
JDBC_BATCH_SIZE = int(os.getenv("JDBC_BATCH_SIZE", "5000"))

JDBC_PROPERTIES = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": JDBC_DRIVER,
    "batchsize": str(JDBC_BATCH_SIZE),
}


def pg_connect_kwargs() -> dict:
    """Connection kwargs for psycopg2 (the Python-side client used for the
    ON CONFLICT upsert in Phase 7 and by the integration tests).

    Separate from JDBC_PROPERTIES on purpose: that one configures Spark's JVM
    driver for the bulk write, this one configures the small transactional
    statement that runs afterwards. Two different drivers, two different worlds.
    """
    return {
        "host": PG_HOST,
        "port": PG_PORT,
        "dbname": PG_DB,
        "user": PG_USER,
        "password": PG_PASSWORD,
    }


# --- Table names ---

EVENTS_TABLE = "events"
QUARANTINE_TABLE = "events_quarantine"
METRICS_TABLE = "event_metrics"

# --- Streaming knobs (varied by the Phase 8 load matrix) ---

# Bounds how many files one micro-batch consumes. Without it the FIRST batch
# tries to eat the entire backlog sitting in incoming/, which turns a restart
# after a long generator run into a multi-minute batch that can exhaust memory.
MAX_FILES_PER_TRIGGER = int(os.getenv("MAX_FILES_PER_TRIGGER", "20"))

# How often a micro-batch fires. A fixed interval gives predictable batch
# boundaries and an even metric series; the default (no trigger) starts the next
# batch the instant the last ends, which makes the latency report noisy.
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "10 seconds")

# Spark's file-source metadata log only considers a consumed file eligible for
# cleanSource archiving/deletion after this delay has elapsed since it was
# recorded. Confirmed directly by testing against the real container: the
# DEFAULT is 600000ms (10 minutes) -- a sensible safety margin in a general
# cluster, but it would make cleanSource=archive invisible in any demo or test
# running for less than ten minutes. Also confirmed directly: this is a
# "static" config -- it only takes effect if set when the SparkSession is
# BUILT (see spark_session.py's use of this constant); calling
# spark.conf.set(...) on an already-created session has no effect at all,
# silently. Safe to set this low here specifically because nothing in this
# project ever reads an incoming/ file a second time once Spark has committed
# the batch that consumed it.
FILE_SOURCE_CLEANUP_DELAY = os.getenv("FILE_SOURCE_CLEANUP_DELAY", "0s")

# --- Spark session defaults ---

SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "rtdi-ecommerce-streaming")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

# The 200 default is pure overhead at this data size, and it is actively
# dangerous here: df.write.jdbc opens one connection PER PARTITION, and 200
# concurrent connections against Postgres's default max_connections of 100
# fails the job or takes the database down for everything else.
SHUFFLE_PARTITIONS = int(os.getenv("SHUFFLE_PARTITIONS", "4"))

# Everything -- generator, Spark, Postgres -- speaks UTC. Event timestamps are
# written ISO-8601 UTC, and pinning the session timezone stops Spark silently
# reinterpreting them in the container's local zone, which would shift every
# windowed aggregate and make the results wrong in a way that looks plausible.
SPARK_TIMEZONE = os.getenv("SPARK_TIMEZONE", "UTC")
