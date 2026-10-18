"""A logging.Handler that ships each record as a log line, with its own event_id and the record's time."""

from __future__ import annotations

import logging
import threading
import traceback
from typing import Any, Dict, Optional

from ._events import IDENTITY_FIELDS, scalar_prop_value

# The most of a stack trace kept in props.stacktrace (its innermost end).
MAX_STACKTRACE_CHARS = 8192

# Attributes every LogRecord has; anything else came from `extra=` and becomes a prop.
STANDARD_RECORD_ATTRIBUTES = frozenset(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {"message", "asctime", "taskName"}


def level_for_record(level_number: int) -> str:
    """The ingest API level for a logging level number; custom levels map to the nearest below."""
    if level_number < logging.INFO:
        return "debug"

    if level_number < logging.WARNING:
        return "info"

    if level_number < logging.ERROR:
        return "warning"

    if level_number < logging.CRITICAL:
        return "error"

    return "critical"


class SimpleDataLogsHandler(logging.Handler):
    """Queues every record on a client; it never blocks on the network and never raises into the app."""

    def __init__(self, client: Any = None, level: int = logging.NOTSET, *, include_extras: bool = True) -> None:
        """Takes a Client or AsyncClient (the one from simpledatalogs.init() when None) and a minimum level."""
        super().__init__(level)
        self.client = client
        self.include_extras = include_extras
        self._emitting = threading.local()

    def emit(self, record: logging.LogRecord) -> None:
        """Turns the record into a log line; the SDK's own warnings are never shipped, so it can't loop."""
        if record.name == "simpledatalogs" or record.name.startswith("simpledatalogs."):
            return

        # A log call made while shipping a record (from a prop's __str__, say) is skipped.
        if getattr(self._emitting, "is_active", False):
            return

        self._emitting.is_active = True

        try:
            client = self.client if self.client is not None else _default_client()

            if client is None:
                return

            props, identities = self._props_and_identities(record)
            client.log(level_for_record(record.levelno), _record_message(record), props, timestamp=record.created, **identities)
        except Exception:
            pass
        finally:
            self._emitting.is_active = False

    # The logger name, the exception and scalar extras as props; identity extras become identity fields.
    def _props_and_identities(self, record: logging.LogRecord) -> "tuple[Dict[str, Any], Dict[str, Any]]":
        props: Dict[str, Any] = {"logger": record.name}
        identities: Dict[str, Any] = {}

        # An exception keeps its type, message and the end of its trace.
        if record.exc_info and record.exc_info[1] is not None:
            exception = record.exc_info[1]
            props["exception.type"] = type(exception).__name__
            props["exception.message"] = str(exception)
            props["stacktrace"] = "".join(traceback.format_exception(*record.exc_info))[-MAX_STACKTRACE_CHARS:]

        if not self.include_extras:
            return props, identities

        # Values passed with extra={...}: identity fields fill identity, scalars become props.
        for attribute_name, attribute_value in record.__dict__.items():
            if attribute_name in STANDARD_RECORD_ATTRIBUTES or attribute_name.startswith("_"):
                continue

            if attribute_name in IDENTITY_FIELDS and attribute_name != "install_id":
                identities[attribute_name] = attribute_value
                continue

            if scalar_prop_value(attribute_value) is not None:
                props[attribute_name] = attribute_value

        return props, identities


# The record's formatted message; a broken format string falls back to the raw message.
def _record_message(record: logging.LogRecord) -> str:
    try:
        return record.getMessage()
    except Exception:
        return str(record.msg)


# The client set up by simpledatalogs.init(), resolved late so the handler can be configured first.
def _default_client() -> Optional[Any]:
    from . import get_client

    return get_client()
