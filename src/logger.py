"""Shared logging for every stage of the pipeline (generator, streaming job,
transforms, sinks) and for the inline validation each of those writes for itself.

Two log files exist in this project and they are deliberately separate:

    logs/pipeline.log   - the human narrative, written here
    logs/metrics.jsonl  - machine-parseable per-batch numbers, written by
                          src/monitoring.py (Phase 8)

Mixing them makes both worse: you cannot `jq` a file full of prose, and you
cannot read a file full of JSON. Keep the split.

pipeline.log accumulates across runs (FileHandler appends, nothing rotates or
truncates it), so it reads as the full history of every generator and streaming
invocation rather than just the most recent one. `make clean` is what resets it.
"""

import logging
import os
import sys

LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "pipeline.log")

_LOG_FORMAT = "[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


class _ColorFormatter(logging.Formatter):
    """Wraps WARNING/ERROR lines in ANSI colour so a quarantined-row warning or a
    failed batch stands out while scrolling past routine INFO lines (files
    written, batch committed, row counts).

    Console-only: never applied to the file handler below, since raw escape
    codes would show up as garbled control characters when logs/pipeline.log is
    opened in a plain text editor rather than a terminal.
    """

    _COLOR_BY_LEVEL = {
        logging.WARNING: "\033[33m",   # yellow
        logging.ERROR: "\033[31m",     # red
        logging.CRITICAL: "\033[1;31m",  # bold red
    }
    _RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        message = super().format(record)
        color = self._COLOR_BY_LEVEL.get(record.levelno)
        return f"{color}{message}{self._RESET}" if color else message


def get_logger(name: str = "rtdi_pipeline") -> logging.Logger:
    """Return a logger under `name` writing to both stdout and logs/pipeline.log.

    Safe to call repeatedly with the same name (e.g. once per module import):
    `logging.getLogger(name)` always returns the same singleton instance, so the
    `if not logger.handlers` guard is what stops handlers piling up and
    double-printing every line.
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        logger.setLevel(logging.INFO)
        plain_formatter = logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT)

        # 1. Console - what you watch while the stream runs.
        # Colour only for a real terminal: piping or redirecting (make logs
        # collected to a file, a notebook cell, CI) falls back to plain so
        # escape codes never get baked into captured text.
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(
            _ColorFormatter(_LOG_FORMAT, datefmt=_DATE_FORMAT)
            if sys.stdout.isatty()
            else plain_formatter
        )
        logger.addHandler(console_handler)

        # 2. File - the same lines, persisted, always plain text.
        file_handler = logging.FileHandler(LOG_FILE)
        file_handler.setFormatter(plain_formatter)
        logger.addHandler(file_handler)

        # Stop records also bubbling to the root logger. PySpark (and anything
        # else in the container) may attach handlers to root, and without this
        # every line would print twice once they do.
        logger.propagate = False

    return logger
