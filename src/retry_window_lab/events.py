"""Structured process events. Library calls emit through Python logging."""

from datetime import UTC, datetime
import json
import logging
import sys

LOGGER_NAME = "retry_window_lab.events"
logger = logging.getLogger(LOGGER_NAME)


def configure_event_output():
    if not any(handler.name == "rwl-json" for handler in logger.handlers):
        handler = logging.StreamHandler(sys.stdout)
        handler.name = "rwl-json"
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False


def emit_event(event, **fields):
    if logger.isEnabledFor(logging.INFO):
        logger.info(json.dumps({
            "timestamp": datetime.now(UTC).isoformat(timespec="milliseconds"),
            "event": event,
            **fields,
        }, default=str, ensure_ascii=True))
