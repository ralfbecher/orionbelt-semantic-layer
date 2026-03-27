"""Structured logging configuration using structlog."""

from __future__ import annotations

import logging
import sys

import structlog

_LOGGER_RENAMES = {"uvicorn.error": "uvicorn", "uvicorn.access": "uvicorn.access"}


def _rename_loggers(
    logger: logging.Logger, method_name: str, event_dict: structlog.types.EventDict
) -> structlog.types.EventDict:
    """Rename misleading logger names (e.g. uvicorn.error → uvicorn)."""
    name = event_dict.get("logger")
    if name and name in _LOGGER_RENAMES:
        event_dict["logger"] = _LOGGER_RENAMES[name]
    return event_dict


def configure_logging(log_level: str = "INFO", log_format: str = "console") -> None:
    """Configure structlog for the application.

    Args:
        log_level: Standard logging level (DEBUG, INFO, WARNING, ERROR).
        log_format: "console" for local dev, "json" for structured logging,
            "cloudrun" for JSON without uvicorn access logs (Cloud Run provides its own).
    """
    use_json = log_format in ("json", "cloudrun")

    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        _rename_loggers,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]

    if use_json:
        renderer: structlog.types.Processor = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer()

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
        foreign_pre_chain=shared_processors,
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(log_level.upper())

    # Quiet noisy third-party loggers
    logging.getLogger("httpx").setLevel(logging.WARNING)
