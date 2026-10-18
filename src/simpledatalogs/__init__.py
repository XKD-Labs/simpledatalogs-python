"""simpledatalogs for Python: send events and log lines to the ingest API without blocking or breaking the app."""

from __future__ import annotations

import threading
from typing import Any, Mapping, Optional

from ._client import DEFAULT_ENDPOINT, AsyncClient, Client
from ._events import Timestamp
from ._logging import SimpleDataLogsHandler
from ._version import __version__

__all__ = [
    "DEFAULT_ENDPOINT",
    "AsyncClient",
    "Client",
    "SimpleDataLogsHandler",
    "Timestamp",
    "__version__",
    "close",
    "flush",
    "get_client",
    "init",
    "log",
    "track",
]

# The process-wide client from init(), for apps that don't pass one around.
_default_client: Optional[Client] = None
_default_client_lock = threading.Lock()


def init(key: Optional[str] = None, **options: Any) -> Client:
    """Creates the process-wide client (closing any earlier one) and returns it."""
    global _default_client

    with _default_client_lock:
        previous_client = _default_client
        _default_client = Client(key, **options)

    if previous_client is not None:
        previous_client.close()

    return _default_client


def get_client() -> Optional[Client]:
    """The client from init(), or None before it."""
    return _default_client


def track(event: str, props: Optional[Mapping[str, Any]] = None, **options: Any) -> None:
    """Queues an event on the client from init(); does nothing before init()."""
    if _default_client is not None:
        _default_client.track(event, props, **options)


def log(level: str, message: Any, props: Optional[Mapping[str, Any]] = None, **options: Any) -> None:
    """Queues a log line on the client from init(); does nothing before init()."""
    if _default_client is not None:
        _default_client.log(level, message, props, **options)


def flush(timeout: Optional[float] = 5.0) -> bool:
    """Flushes the client from init(); true when nothing is left."""
    return True if _default_client is None else _default_client.flush(timeout)


def close(timeout: Optional[float] = 5.0) -> bool:
    """Closes the client from init(); true when nothing was left."""
    return True if _default_client is None else _default_client.close(timeout)

