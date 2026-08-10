#!/usr/bin/env python
"""CLI entrypoint for the event generator.
"""

import argparse

from src.generator import run


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Emit fake e-commerce events as CSV files for Spark Structured Streaming to consume."
    )
    parser.add_argument("--rate", type=float, default=50.0, help="target events per second (default: 50)")
    parser.add_argument("--batch-size", type=int, default=100, help="events per CSV file (default: 100)")
    parser.add_argument(
        "--duration", type=float, default=None, help="stop after this many seconds (default: run until Ctrl+C)"
    )
    parser.add_argument(
        "--bad-rate", type=float, default=0.02, help="fraction of deliberately malformed events (default: 0.02)"
    )
    parser.add_argument(
        "--seed", type=int, default=None, help="seed for reproducible output (default: real random data)"
    )
    args = parser.parse_args()

    run(
        rate=args.rate,
        batch_size=args.batch_size,
        duration=args.duration,
        bad_rate=args.bad_rate,
        seed=args.seed,
    )


if __name__ == "__main__":
    main()
