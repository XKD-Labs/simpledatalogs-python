"""The retry policy and what each ingest answer does to the queue."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import List, Tuple

from simpledatalogs._pipeline import DROPPED, FAILED, SENT, SPLIT, Limits, Pipeline
from simpledatalogs._retry import parse_retry_after, retry_delay_seconds
from simpledatalogs._transport import Answer


class FakeClock:
    """A monotonic clock the test moves by hand."""

    def __init__(self) -> None:
        """Starts at 1000 s."""
        self.now = 1000.0

    def __call__(self) -> float:
        """The current fake time."""
        return self.now


def build_pipeline(jitter: float = 1.0, **limit_overrides: int) -> Tuple[Pipeline, FakeClock, List[str]]:
    """A pipeline with a fake clock and fixed jitter, recording warning kinds."""
    limits = {"max_batch_events": 50, "max_request_bytes": 256_000, "max_queue_events": 10_000, "max_queue_bytes": 10_000_000}
    limits.update(limit_overrides)
    clock = FakeClock()
    warned_kinds: List[str] = []
    pipeline = Pipeline(Limits(**limits), lambda kind, message: warned_kinds.append(kind), clock=clock, random_fraction=lambda: jitter)
    return pipeline, clock, warned_kinds


def lines(count: int, size: int = 10) -> List[bytes]:
    """Distinct lines of the given size."""
    return [(b"%05d" % index).ljust(size, b"x") for index in range(count)]


def test_backoff_grows_from_one_second_and_caps_at_five_minutes() -> None:
    assert retry_delay_seconds(1, None, 1.0) == 1.0
    assert retry_delay_seconds(2, None, 1.0) == 2.0
    assert retry_delay_seconds(4, None, 1.0) == 8.0
    assert retry_delay_seconds(50, None, 1.0) == 300.0


def test_backoff_is_full_jitter_with_a_floor() -> None:
    assert retry_delay_seconds(5, None, 0.5) == 8.0
    assert retry_delay_seconds(5, None, 0.0) == 0.25


def test_retry_after_is_a_floor_capped_at_an_hour() -> None:
    assert retry_delay_seconds(1, 30.0, 0.1) == 30.0
    assert retry_delay_seconds(1, 86_400.0, 0.1) == 3600.0


def test_retry_after_reads_seconds_and_http_dates() -> None:
    now = datetime(2026, 10, 19, 12, 0, 0, tzinfo=timezone.utc)
    in_two_minutes = (now + timedelta(minutes=2)).strftime("%a, %d %b %Y %H:%M:%S GMT")

    assert parse_retry_after("120") == 120.0
    assert parse_retry_after(in_two_minutes, now) == 120.0
    assert parse_retry_after("Mon, 01 Jan 2001 00:00:00 GMT", now) == 0.0
    assert parse_retry_after("soon") is None
    assert parse_retry_after(None) is None


def test_a_202_removes_the_batch_and_resets_the_backoff() -> None:
    pipeline, clock, _ = build_pipeline()
    pipeline.enqueue(lines(3))
    batch = pipeline.next_batch()

    assert pipeline.apply_answer(batch, Answer(503, None)) == FAILED
    assert pipeline.wait_seconds() == 1.0

    clock.now += 1.0
    assert pipeline.apply_answer(batch, Answer(202, None)) == SENT
    assert pipeline.pending_count == 0
    assert pipeline.wait_seconds() == 0.0


def test_503_and_429_honor_retry_after_and_keep_the_batch() -> None:
    pipeline, _, _ = build_pipeline(jitter=0.0)
    pipeline.enqueue(lines(2))

    assert pipeline.apply_answer(pipeline.next_batch(), Answer(503, 42.0)) == FAILED
    assert pipeline.wait_seconds() == 42.0
    assert pipeline.pending_count == 2

    assert pipeline.apply_answer(pipeline.next_batch(), Answer(429, 3600.0)) == FAILED
    assert pipeline.wait_seconds() == 3600.0
    assert pipeline.pending_count == 2


def test_every_non_202_status_is_retried() -> None:
    for status in (200, 204, 301, 400, 403, 404, 408, 500, 502, 504):
        pipeline, _, _ = build_pipeline()
        pipeline.enqueue(lines(1))

        assert pipeline.apply_answer(pipeline.next_batch(), Answer(status, None)) == FAILED
        assert pipeline.pending_count == 1


def test_a_401_pauses_a_minute_then_ten_minutes_and_keeps_everything() -> None:
    pipeline, clock, warned_kinds = build_pipeline()
    pipeline.enqueue(lines(5))

    pipeline.apply_answer(pipeline.next_batch(), Answer(401, None))
    assert pipeline.wait_seconds() == 60.0

    clock.now += 60.0
    pipeline.apply_answer(pipeline.next_batch(), Answer(401, None))
    assert pipeline.wait_seconds() == 600.0
    assert pipeline.pending_count == 5
    assert "invalid_key" in warned_kinds


def test_a_413_halves_the_batch_and_a_202_doubles_it_back() -> None:
    pipeline, _, _ = build_pipeline(max_batch_events=8)
    pipeline.enqueue(lines(20))

    assert pipeline.apply_answer(pipeline.next_batch(), Answer(413, None)) == SPLIT
    assert len(pipeline.next_batch()) == 4

    assert pipeline.apply_answer(pipeline.next_batch(), Answer(202, None)) == SENT
    assert len(pipeline.next_batch()) == 8


def test_a_413_for_a_single_line_drops_only_that_line() -> None:
    pipeline, _, warned_kinds = build_pipeline(max_batch_events=1)
    pipeline.enqueue(lines(2))

    assert pipeline.apply_answer(pipeline.next_batch(), Answer(413, None)) == DROPPED
    assert pipeline.pending_count == 1
    assert "rejected_413" in warned_kinds


def test_a_415_drops_the_batch_once_warned() -> None:
    pipeline, _, warned_kinds = build_pipeline(max_batch_events=2)
    pipeline.enqueue(lines(3))

    assert pipeline.apply_answer(pipeline.next_batch(), Answer(415, None)) == DROPPED
    assert pipeline.pending_count == 1
    assert warned_kinds == ["rejected_415"]


def test_batches_respect_the_count_and_byte_caps() -> None:
    pipeline, _, _ = build_pipeline(max_batch_events=50, max_request_bytes=1024)
    pipeline.enqueue(lines(40, size=99))

    assert len(pipeline.next_batch()) == 10

    pipeline, _, _ = build_pipeline(max_batch_events=7)
    pipeline.enqueue(lines(40))

    assert len(pipeline.next_batch()) == 7


def test_the_queue_drops_the_oldest_over_its_caps() -> None:
    pipeline, _, warned_kinds = build_pipeline(max_queue_events=5)
    pipeline.enqueue(lines(8))

    assert pipeline.pending_count == 5
    assert pipeline.next_batch()[0].line.startswith(b"00003")
    assert warned_kinds == ["queue_full"]

    pipeline, _, _ = build_pipeline(max_queue_bytes=1100)
    pipeline.enqueue(lines(20, size=99))

    assert pipeline.pending_count == 11


def test_lines_queued_during_a_send_survive_its_202() -> None:
    pipeline, _, _ = build_pipeline()
    pipeline.enqueue(lines(2))
    batch = pipeline.next_batch()

    pipeline.enqueue([b"late"])
    pipeline.apply_answer(batch, Answer(202, None))

    assert [entry.line for entry in pipeline.next_batch()] == [b"late"]
