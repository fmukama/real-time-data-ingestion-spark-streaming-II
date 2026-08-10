#!/usr/bin/env python
"""CLI entrypoint for the Spark Structured Streaming job.
"""

from src.logger import get_logger
from src.spark_session import get_spark_session, stop_spark_session
from src.streaming import start_query

logger = get_logger("streaming_job")


def main() -> None:
    spark = get_spark_session()
    query = start_query(spark)

    logger.info("streaming job running -- Ctrl+C to stop.")
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("streaming job stopping (Ctrl+C)")
        query.stop()
    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    main()
