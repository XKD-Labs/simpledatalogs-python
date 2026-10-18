"""The sending state shared by both clients: a bounded queue, batches, and what each ingest answer does."""

from __future__ import annotations

import collections
import random
import threading
import time
from dataclasses import dataclass
from typing import Callable, Deque, List, Optional

from ._outbox import DiskOutbox
from ._retry import (
    INVALID_KEY_FIRST_PROBE_SECONDS,
    INVALID_KEY_PROBE_SECONDS,
    honor_retry_after,
    retry_delay_seconds,
)
from ._transport import Answer

# What one batch's answer meant for the drain loop.
SENT = "sent"
SPLIT = "split"
DROPPED = "dropped"
FAILED = "failed"


@dataclass(frozen=True)
class Limits:
    """The batch and queue caps."""

    max_batch_events: int
    max_request_bytes: int
    max_queue_events: int
    max_queue_bytes: int


class Entry:
    """One queued NDJSON line; compared by identity, so equal lines stay distinct."""

    __slots__ = ("line",)

    def __init__(self, line: bytes) -> None:
        """Keeps the encoded line, without its newline."""
        self.line = line

    @property
    def size(self) -> int:
        """The line's bytes in a body, newline included."""
        return len(self.line) + 1


class Pipeline:
    """Holds the queue and the retry state; every method is safe to call from any thread."""

    def __init__(
        self,
        limits: Limits,
        warn: Callable[[str, str], None],
        outbox: Optional[DiskOutbox] = None,
        clock: Callable[[], float] = time.monotonic,
        random_fraction: Callable[[], float] = random.random,
    ) -> None:
        """Keeps the caps, the warning callback, the optional disk outbox, and a swappable clock and randomness."""
        self.limits = limits
        self.outbox = outbox
        self.lock = threading.Lock()
        self._warn = warn
        self._clock = clock
        self._random_fraction = random_fraction
        self._queue: Deque[Entry] = collections.deque()
        self._queued_bytes = 0
        self._failure_count = 0
        self._invalid_key_probe_count = 0
        self._backoff_until = 0.0
        self._invalid_key_until = 0.0
        self._current_batch_events = limits.max_batch_events

    def enqueue(self, lines: List[bytes]) -> bool:
        """Queues lines (and writes them to disk first); true when a full batch is waiting."""
        with self.lock:
            # Disk first, so a crash right after still has them.
            if self.outbox is not None:
                self._write_to_outbox(lines)

            for line in lines:
                self._queue.append(Entry(line))
                self._queued_bytes += len(line) + 1

            self._trim_queue()

            return len(self._queue) >= self._current_batch_events

    @property
    def pending_count(self) -> int:
        """Lines not yet acknowledged with a 202."""
        return len(self._queue)

    def clear(self) -> None:
        """Forgets every queued line (used in a forked child, whose parent still owns them)."""
        with self.lock:
            self._queue.clear()
            self._queued_bytes = 0

    def next_batch(self) -> List[Entry]:
        """The oldest lines that fit one request, by count and by bytes; at least one when any wait."""
        with self.lock:
            batch_entries: List[Entry] = []
            body_bytes = 0

            for entry in self._queue:
                if len(batch_entries) >= self._current_batch_events:
                    break

                if batch_entries and body_bytes + entry.size > self.limits.max_request_bytes:
                    break

                batch_entries.append(entry)
                body_bytes += entry.size

            return batch_entries

    def wait_seconds(self) -> float:
        """Seconds until sending is allowed again; zero when it is allowed now."""
        with self.lock:
            return max(0.0, max(self._backoff_until, self._invalid_key_until) - self._clock())

    def apply_answer(self, batch_entries: List[Entry], answer: Answer) -> str:
        """Applies one ingest answer: 202 removes, 413 splits, 415 drops, 401 pauses, anything else backs off."""
        with self.lock:
            # Only a 202 means stored: the batch leaves, the counters reset, and the batch size grows back after a 413.
            if answer.status == 202:
                self._remove(batch_entries)
                self._failure_count = 0
                self._invalid_key_probe_count = 0
                self._current_batch_events = min(self.limits.max_batch_events, self._current_batch_events * 2)
                return SENT

            # Unsupported as sent: resending unchanged can never work, so the batch is dropped.
            if answer.status == 415:
                self._warn("rejected_415", "ingest refused a batch as unsupported (415); it was dropped")
                self._remove(batch_entries)
                return DROPPED

            # Too large: halve the batch, or drop a lone line that can never be accepted.
            if answer.status == 413:
                if len(batch_entries) > 1:
                    self._current_batch_events = max(1, len(batch_entries) // 2)
                    return SPLIT

                self._warn("rejected_413", "ingest refused a line as too large (413); it was dropped")
                self._remove(batch_entries)
                return DROPPED

            # Unknown or revoked key: keep everything, re-probe after a minute and then every 10 minutes.
            if answer.status == 401:
                self._warn("invalid_key", "the ingest key was refused (401); lines are kept and the key is re-checked every 10 minutes")
                probe_delay_seconds = INVALID_KEY_FIRST_PROBE_SECONDS if self._invalid_key_probe_count == 0 else INVALID_KEY_PROBE_SECONDS
                self._invalid_key_probe_count += 1
                self._invalid_key_until = self._clock() + honor_retry_after(probe_delay_seconds, answer.retry_after_seconds)
                return FAILED

            # Anything else (408, 429, 503, 5xx, a redirect) wasn't stored: back off and retry.
            self._back_off(answer.retry_after_seconds)
            return FAILED

    def apply_network_error(self) -> str:
        """A timeout or network error: unknown whether it was stored, so keep it and back off."""
        with self.lock:
            self._back_off(None)
            return FAILED

    # Pauses sending for a full-jitter backoff that is never shorter than Retry-After.
    def _back_off(self, retry_after_seconds: Optional[float]) -> None:
        self._failure_count += 1
        delay_seconds = retry_delay_seconds(self._failure_count, retry_after_seconds, self._random_fraction())
        self._backoff_until = self._clock() + delay_seconds

    # Removes the given entries, wherever they are now, and tells the disk outbox.
    def _remove(self, batch_entries: List[Entry]) -> None:
        batch_ids = {id(entry) for entry in batch_entries}
        self._queue = collections.deque(entry for entry in self._queue if id(entry) not in batch_ids)
        self._queued_bytes = sum(entry.size for entry in self._queue)

        if self.outbox is not None:
            self._acknowledge_on_disk(sum(entry.size for entry in batch_entries))

    # Drops the oldest lines while the queue is over its line or byte cap.
    def _trim_queue(self) -> None:
        dropped_count = 0

        while self._queue and (len(self._queue) > self.limits.max_queue_events or self._queued_bytes > self.limits.max_queue_bytes):
            self._queued_bytes -= self._queue.popleft().size
            dropped_count += 1

        if dropped_count:
            self._warn("queue_full", f"the queue is over its cap; dropped the oldest {dropped_count} unsent lines")

    # Appends to disk; a disk failure turns the outbox off and the queue carries on in memory.
    def _write_to_outbox(self, lines: List[bytes]) -> None:
        try:
            self.outbox.append(lines)  # type: ignore[union-attr]
        except OSError as error:
            self._warn("outbox_write", f"the disk outbox failed and is off; lines stay in memory: {error}")
            self.outbox = None

    # Tells the disk outbox what was acknowledged; a disk failure turns it off.
    def _acknowledge_on_disk(self, acknowledged_bytes: int) -> None:
        try:
            self.outbox.acknowledge(acknowledged_bytes, [entry.line for entry in self._queue])  # type: ignore[union-attr]
        except OSError as error:
            self._warn("outbox_write", f"the disk outbox failed and is off; lines stay in memory: {error}")
            self.outbox = None
