"""The logging.Handler and the asyncio client."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Callable

import simpledatalogs
from conftest import TEST_KEY
from fake_ingest import FakeIngest


def build_logger(handler: logging.Handler, name: str) -> logging.Logger:
    """A logger that sends only to the given handler."""
    logger = logging.getLogger(name)
    logger.handlers = [handler]
    logger.propagate = False
    logger.setLevel(1)
    return logger


def test_records_become_log_lines_with_level_time_and_extras(fake_ingest: FakeIngest, make_client: Callable[..., Any]) -> None:
    client = make_client()
    logger = build_logger(simpledatalogs.SimpleDataLogsHandler(client), "app.payments")

    logger.warning("Payment failed for order %s", 7, extra={"order_id": 7, "user_id": 42, "context": {"skipped": True}})
    client.flush()

    line = fake_ingest.accepted_lines[0]
    assert line["level"] == "warning"
    assert line["message"] == "Payment failed for order 7"
    assert line["user_id"] == "42"
    assert line["props"] == {"logger": "app.payments", "order_id": 7}
    assert abs(datetime.fromisoformat(line["ts"]) - datetime.now(timezone.utc)).total_seconds() < 60


def test_exceptions_keep_type_message_and_trace(fake_ingest: FakeIngest, make_client: Callable[..., Any]) -> None:
    client = make_client()
    logger = build_logger(simpledatalogs.SimpleDataLogsHandler(client), "app.errors")

    try:
        raise ValueError("card declined")
    except ValueError:
        logger.exception("Checkout crashed")

    client.flush()

    props = fake_ingest.accepted_lines[0]["props"]
    assert fake_ingest.accepted_lines[0]["level"] == "error"
    assert props["exception.type"] == "ValueError"
    assert props["exception.message"] == "card declined"
    assert "Traceback" in props["stacktrace"]


def test_levels_map_to_the_ingest_levels(fake_ingest: FakeIngest, make_client: Callable[..., Any]) -> None:
    client = make_client()
    logger = build_logger(simpledatalogs.SimpleDataLogsHandler(client), "app.levels")

    for level_number in (5, logging.DEBUG, logging.INFO, 25, logging.WARNING, logging.ERROR, logging.CRITICAL):
        logger.log(level_number, "level %s", level_number)

    client.flush()

    assert [line["level"] for line in fake_ingest.accepted_lines] == ["debug", "debug", "info", "info", "warning", "error", "critical"]


def test_the_sdk_never_ships_its_own_warnings(fake_ingest: FakeIngest, make_client: Callable[..., Any]) -> None:
    client = make_client()
    handler = simpledatalogs.SimpleDataLogsHandler(client)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)

    try:
        client.track("")
        logging.getLogger("simpledatalogs").warning("loop?")
    finally:
        root_logger.removeHandler(handler)

    assert client.pending_count == 0


def test_the_handler_uses_the_init_client_by_default(fake_ingest: FakeIngest) -> None:
    simpledatalogs.init(TEST_KEY, endpoint=fake_ingest.endpoint, flush_at_exit=False)
    logger = build_logger(simpledatalogs.SimpleDataLogsHandler(), "app.default")

    logger.info("through the default client")
    simpledatalogs.close()

    assert fake_ingest.accepted_lines[0]["message"] == "through the default client"


def test_the_async_client_sends_from_its_task(fake_ingest: FakeIngest) -> None:
    async def run() -> None:
        async with simpledatalogs.AsyncClient(TEST_KEY, endpoint=fake_ingest.endpoint, flush_at_exit=False, flush_interval_seconds=0.1) as client:
            client.track("async_signup", {"plan": "pro"})
            await asyncio.sleep(0.5)
            assert len(fake_ingest.accepted_lines) == 1

            client.log("info", "flushed on close")

    asyncio.run(run())

    assert [line.get("event", line.get("message")) for line in fake_ingest.accepted_lines] == ["async_signup", "flushed on close"]


def test_the_async_client_retries_with_the_same_event_id(fake_ingest: FakeIngest) -> None:
    fake_ingest.answers = [(503, {"Retry-After": "0"}), (202, {})]

    async def run() -> bool:
        client = simpledatalogs.AsyncClient(TEST_KEY, endpoint=fake_ingest.endpoint, flush_at_exit=False, flush_interval_seconds=60)
        client.track("async_retry")
        is_delivered = await client.flush(timeout=5)
        await client.aclose()
        return is_delivered

    assert asyncio.run(run()) is True
    assert fake_ingest.requests[0].lines == fake_ingest.requests[1].lines


def test_the_async_client_keeps_lines_on_401(fake_ingest: FakeIngest) -> None:
    fake_ingest.answers = [(401, {})]

    async def run() -> int:
        client = simpledatalogs.AsyncClient(TEST_KEY, endpoint=fake_ingest.endpoint, flush_at_exit=False)
        client.track("kept")
        await client.flush(timeout=1)
        return client.pending_count

    assert asyncio.run(run()) == 1


def test_dict_config_builds_the_handler_by_its_import_path(fake_ingest: FakeIngest) -> None:
    import logging.config

    simpledatalogs.init(TEST_KEY, endpoint=fake_ingest.endpoint, flush_at_exit=False)
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "handlers": {"simpledatalogs": {"class": "simpledatalogs.SimpleDataLogsHandler", "level": "WARNING"}},
            "loggers": {"app.configured": {"handlers": ["simpledatalogs"], "level": "INFO", "propagate": False}},
        }
    )

    logging.getLogger("app.configured").info("below the handler level")
    logging.getLogger("app.configured").warning("configured by dictConfig")
    simpledatalogs.close()

    assert [line["message"] for line in fake_ingest.accepted_lines] == ["configured by dictConfig"]
