"""The threaded client against a fake ingest: wire format, retries, caps and failing open."""

from __future__ import annotations

import enum
import gzip
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable

import pytest

import simpledatalogs
from fake_ingest import FakeIngest


class Plan(enum.Enum):
    """An enum prop, sent as its value."""

    PRO = "pro"


def test_a_tracked_event_reaches_ingest_with_the_contract_headers(fake_ingest: FakeIngest, make_client: Callable[..., Any]) -> None:
    client = make_client()
    client.track("signup", {"plan": Plan.PRO, "seats": 3, "trial": False}, user_id=42)

    assert client.flush() is True

    request = fake_ingest.requests[0]
    assert request.path == "/v1/ingest"
    assert request.headers["authorization"] == "Bearer sdl_sec_test_0000000000000000"
    assert request.headers["content-type"] == "application/x-ndjson"
    assert request.headers["user-agent"] == f"simpledatalogs-python/{simpledatalogs.__version__}"

    line = request.lines[0]
    assert line["event"] == "signup"
    assert line["user_id"] == "42"
    assert line["props"] == {"plan": "pro", "seats": 3, "trial": False}
    assert uuid.UUID(line["event_id"]).version == 4
    assert datetime.fromisoformat(line["ts"]).tzinfo is not None


def test_event_id_and_ts_are_set_before_the_first_attempt_and_kept_on_retries(fake_ingest: FakeIngest, make_client: Callable[..., Any]) -> None:
    fake_ingest.answers = [(503, {"Retry-After": "0"}), (500, {}), (202, {})]
    client = make_client()
    client.track("checkout")
    time.sleep(0.01)

    assert client.flush(timeout=10) is True

    sent_lines = [request.lines[0] for request in fake_ingest.requests]
    assert len(sent_lines) == 3
    assert len({line["event_id"] for line in sent_lines}) == 1
    assert len({line["ts"] for line in sent_lines}) == 1


def test_large_bodies_are_gzipped_and_stay_under_the_limits(fake_ingest: FakeIngest, make_client: Callable[..., Any]) -> None:
    client = make_client(max_request_bytes=20_000)

    for index in range(200):
        client.log("info", f"line {index} " + "x" * 300)

    assert client.flush(timeout=10) is True
    assert len(fake_ingest.accepted_lines) == 200

    for request in fake_ingest.requests:
        assert request.headers["content-encoding"] == "gzip"
        assert len(request.body) <= 20_000
        assert len(request.lines) <= 50
        assert gzip.decompress(request.raw_body) == request.body


def test_small_bodies_are_not_gzipped(fake_ingest: FakeIngest, make_client: Callable[..., Any]) -> None:
    client = make_client()
    client.track("tiny")
    client.flush()

    assert "content-encoding" not in fake_ingest.requests[0].headers


def test_the_request_cap_never_passes_512_kb(make_client: Callable[..., Any]) -> None:
    client = make_client(max_request_bytes=10_000_000)

    assert client._pipeline.limits.max_request_bytes == 524_288


def test_an_oversized_event_is_skipped_and_an_oversized_log_line_is_cut(fake_ingest: FakeIngest, make_client: Callable[..., Any]) -> None:
    client = make_client(max_request_bytes=4096)
    client.track("huge", {"blob": "x" * 10_000})
    client.log("error", "y" * 20_000, {"order_id": 7})

    assert client.flush() is True

    sent_lines = fake_ingest.accepted_lines
    assert len(sent_lines) == 1
    assert sent_lines[0]["props"]["_truncated"] is True
    assert len(sent_lines[0]["message"]) < 4096


def test_the_background_thread_flushes_on_its_interval(fake_ingest: FakeIngest, make_client: Callable[..., Any]) -> None:
    client = make_client(flush_interval_seconds=0.1)
    client.track("background")

    assert fake_ingest.wait_for_requests(1, timeout=5)


def test_a_full_batch_wakes_the_thread_at_once(fake_ingest: FakeIngest, make_client: Callable[..., Any]) -> None:
    client = make_client(flush_interval_seconds=60, max_batch_events=5)

    for index in range(5):
        client.track("burst", {"index": index})

    assert fake_ingest.wait_for_requests(1, timeout=5)


def test_close_flushes_and_later_calls_are_ignored(fake_ingest: FakeIngest, make_client: Callable[..., Any]) -> None:
    client = make_client(flush_interval_seconds=60)
    client.track("last_words")

    assert client.close() is True
    client.track("after_close")

    assert [line["event"] for line in fake_ingest.accepted_lines] == ["last_words"]


def test_a_401_keeps_the_lines_and_flush_reports_them_unsent(fake_ingest: FakeIngest, make_client: Callable[..., Any]) -> None:
    fake_ingest.answers = [(401, {})]
    client = make_client()
    client.track("kept")

    assert client.flush(timeout=1) is False
    assert client.pending_count == 1
    assert len(fake_ingest.requests) == 1


def test_flush_waits_out_retry_after_within_its_timeout(fake_ingest: FakeIngest, make_client: Callable[..., Any]) -> None:
    fake_ingest.answers = [(503, {"Retry-After": "1"}), (202, {})]
    client = make_client()
    client.track("patient")

    started_at = time.monotonic()
    assert client.flush(timeout=5) is True
    assert time.monotonic() - started_at >= 1.0


def test_flush_gives_up_when_retry_after_passes_its_timeout(fake_ingest: FakeIngest, make_client: Callable[..., Any]) -> None:
    fake_ingest.answers = [(429, {"Retry-After": "3600"})]
    client = make_client()
    client.track("rate_limited")

    started_at = time.monotonic()
    assert client.flush(timeout=2) is False
    assert time.monotonic() - started_at < 1.5
    assert client.pending_count == 1


def test_a_network_error_never_raises_and_keeps_the_lines(make_client: Callable[..., Any]) -> None:
    client = make_client(endpoint="http://127.0.0.1:9", request_timeout_seconds=1)
    client.track("offline")

    assert client.flush(timeout=None) is False
    assert client.pending_count == 1


def test_redirects_are_not_followed(fake_ingest: FakeIngest, make_client: Callable[..., Any]) -> None:
    fake_ingest.answers = [(307, {"Location": "http://example.invalid/steal"})]
    client = make_client()
    client.track("stay_here")

    assert client.flush(timeout=None) is False
    assert client.pending_count == 1


def test_bad_input_never_raises(make_client: Callable[..., Any], caplog: pytest.LogCaptureFixture) -> None:
    client = make_client()

    with caplog.at_level(logging.WARNING, logger="simpledatalogs"):
        client.track("")
        client.track(None)  # type: ignore[arg-type]
        client.track("ok", ["not", "a", "mapping"])  # type: ignore[arg-type]
        client.track("ok", {"nested": {"a": 1}, "nan": float("nan"), "kept": 1})
        client.log(None, None)  # type: ignore[arg-type]
        client.log("warn", ValueError("boom"))

    assert client.pending_count == 4
    assert "needs a non-empty event name" in caplog.text


def test_props_are_capped_at_one_hundred(fake_ingest: FakeIngest, make_client: Callable[..., Any]) -> None:
    client = make_client(default_props={"service": "api"})
    client.track("wide", {f"prop_{index}": index for index in range(150)})
    client.flush()

    assert len(fake_ingest.accepted_lines[0]["props"]) == 100


def test_default_props_sit_under_the_call_props(fake_ingest: FakeIngest, make_client: Callable[..., Any]) -> None:
    client = make_client(default_props={"service": "api", "region": "eu"})
    client.track("ping", {"region": "us"})
    client.flush()

    assert fake_ingest.accepted_lines[0]["props"] == {"region": "us", "service": "api"}


def test_log_levels_are_normalized(fake_ingest: FakeIngest, make_client: Callable[..., Any]) -> None:
    client = make_client()
    client.log("WARN", "disk almost full")
    client.log("fatal", "out of memory")
    client.log("custom", "kept as sent")
    client.flush()

    assert [line["level"] for line in fake_ingest.accepted_lines] == ["warning", "critical", "custom"]


def test_timestamps_can_be_given(fake_ingest: FakeIngest, make_client: Callable[..., Any]) -> None:
    client = make_client()
    client.track("backfilled", timestamp=datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc))
    client.track("epoch", timestamp=1_790_000_000)
    client.flush()

    assert [line["ts"] for line in fake_ingest.accepted_lines] == ["2026-01-02T03:04:05.000+00:00", "2026-09-21T14:13:20.000+00:00"]


def test_a_missing_key_disables_the_client_without_raising(monkeypatch: pytest.MonkeyPatch, fake_ingest: FakeIngest) -> None:
    monkeypatch.delenv("SIMPLEDATALOGS_KEY", raising=False)
    client = simpledatalogs.Client(endpoint=fake_ingest.endpoint, flush_at_exit=False)
    client.track("nowhere")

    assert client.pending_count == 0
    assert client.flush() is True
    assert fake_ingest.requests == []


def test_the_key_and_endpoint_come_from_the_environment(monkeypatch: pytest.MonkeyPatch, fake_ingest: FakeIngest) -> None:
    monkeypatch.setenv("SIMPLEDATALOGS_KEY", "sdl_sec_from_env")
    monkeypatch.setenv("SIMPLEDATALOGS_ENDPOINT", fake_ingest.endpoint + "/")

    with simpledatalogs.Client(flush_at_exit=False) as client:
        client.track("configured")

    assert fake_ingest.requests[0].headers["authorization"] == "Bearer sdl_sec_from_env"


def test_the_default_endpoint_is_the_edge_host() -> None:
    client = simpledatalogs.Client("sdl_sec_x", flush_at_exit=False, enabled=False)

    assert client.ingest_url == "https://edge.simpledatalogs.com/v1/ingest"


def test_opting_out_deletes_unsent_lines(fake_ingest: FakeIngest, make_client: Callable[..., Any]) -> None:
    client = make_client(flush_interval_seconds=60)
    client.track("before_opt_out")
    client.set_enabled(False)
    client.track("while_off")

    assert client.pending_count == 0

    client.set_enabled(True)
    client.track("after_opt_in")
    client.flush()

    assert [line["event"] for line in fake_ingest.accepted_lines] == ["after_opt_in"]


def test_module_level_helpers_use_the_init_client(fake_ingest: FakeIngest) -> None:
    simpledatalogs.init("sdl_sec_test", endpoint=fake_ingest.endpoint, flush_at_exit=False)
    simpledatalogs.track("from_module", {"a": 1})
    simpledatalogs.log("info", "hello")

    assert simpledatalogs.close() is True
    assert len(fake_ingest.accepted_lines) == 2
