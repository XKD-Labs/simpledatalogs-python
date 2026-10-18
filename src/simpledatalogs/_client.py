"""The clients: track() and log() only queue; a background thread (or asyncio task) sends."""

from __future__ import annotations

import asyncio
import atexit
import logging
import os
import threading
import time
import weakref
from typing import Any, Dict, List, Mapping, Optional, Tuple, Union, cast

from ._events import (
    MAX_PROPS,
    Timestamp,
    clean_identity,
    clean_props,
    encode_line,
    format_ts,
    merge_props,
    new_event_id,
    normalize_level,
    truncated_log_line,
)
from ._outbox import DiskOutbox, DiskOutboxUnavailable
from ._pipeline import FAILED, Entry, Limits, Pipeline
from ._transport import UrllibTransport

# The production ingest host.
DEFAULT_ENDPOINT = "https://edge.simpledatalogs.com"

# The ingest API refuses bodies over 512 KB, so no batch is allowed past it even uncompressed.
HARD_MAX_REQUEST_BYTES = 524_288

# Where SDK warnings go; the logging handler never ships records from here, so it can't loop.
SDK_LOGGER_NAME = "simpledatalogs"

# What the drain loop does next: send a batch, wait for a pause to end, or stop.
DrainStep = Tuple[str, Union[List[Entry], float, bool]]


class _BaseClient:
    """Settings, event building, the queue and the blocking drain both clients share."""

    def __init__(
        self,
        key: Optional[str] = None,
        *,
        endpoint: Optional[str] = None,
        flush_interval_seconds: float = 5.0,
        max_batch_events: int = 50,
        max_request_bytes: int = 256_000,
        max_queue_events: int = 10_000,
        max_queue_bytes: int = 10_000_000,
        request_timeout_seconds: float = 15.0,
        outbox_dir: Optional[str] = None,
        default_props: Optional[Mapping[str, Any]] = None,
        gzip: bool = True,
        flush_at_exit: bool = True,
        exit_timeout_seconds: float = 5.0,
        enabled: bool = True,
        transport: Any = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """Reads the key and endpoint (or SIMPLEDATALOGS_KEY / SIMPLEDATALOGS_ENDPOINT); a bad setting disables the client, never raises."""
        self._logger = logger or logging.getLogger(SDK_LOGGER_NAME)
        self._warned_kinds: set = set()
        self._warned_lock = threading.Lock()
        self._is_closed = False
        self._flush_interval_seconds = max(float(flush_interval_seconds), 0.05)
        self._exit_timeout_seconds = float(exit_timeout_seconds)

        # The key decides the environment; without one nothing can be sent.
        self._ingest_key = (key if key is not None else os.environ.get("SIMPLEDATALOGS_KEY", "")).strip()
        self._is_enabled = bool(enabled) and self._check_key()

        # The endpoint is the host; the path is the ingest API's.
        endpoint_text = (endpoint or os.environ.get("SIMPLEDATALOGS_ENDPOINT") or DEFAULT_ENDPOINT).strip().rstrip("/")

        if not endpoint_text.startswith(("https://", "http://")):
            self._warn("endpoint", f"the endpoint {endpoint_text!r} is not an http(s) URL; nothing will be sent")
            self._is_enabled = False

        self.ingest_url = f"{endpoint_text}/v1/ingest"

        # Defaults go on every event, under the call's own props.
        self._default_props = clean_props(default_props, self._warn)

        # Caps: every request stays under the API's 512 KB, every queue is bounded.
        limits = Limits(
            max_batch_events=max(int(max_batch_events), 1),
            max_request_bytes=min(max(int(max_request_bytes), 1024), HARD_MAX_REQUEST_BYTES),
            max_queue_events=max(int(max_queue_events), 1),
            max_queue_bytes=max(int(max_queue_bytes), 1024),
        )

        self._transport = transport or UrllibTransport(self.ingest_url, self._ingest_key, float(request_timeout_seconds), should_gzip=gzip)
        self._pipeline = Pipeline(limits, self._warn, outbox=self._open_outbox(outbox_dir))

        # Lines a dead process left on disk go out with ours.
        self._adopt_orphaned_lines()

        if flush_at_exit:
            atexit.register(_close_client_at_exit, weakref.ref(self))

    def track(
        self,
        event: str,
        props: Optional[Mapping[str, Any]] = None,
        *,
        timestamp: Timestamp = None,
        user_id: Any = None,
        device_id: Any = None,
        session_id: Any = None,
        anonymous_id: Any = None,
    ) -> None:
        """Queues an event with optional props and identity; never raises and never blocks on the network."""
        try:
            # An empty name can't become an event.
            if not isinstance(event, str) or event.strip() == "":
                self._warn("event_name", "track() needs a non-empty event name; the event was skipped")
                return

            event_object = {"event": event.strip(), **self._base_fields(timestamp, user_id, device_id, session_id, anonymous_id)}
            self._add_props(event_object, props)
            self._enqueue(event_object, is_log_line=False)
        except Exception as error:
            self._warn("internal", f"internal error, nothing was raised to the app: {error!r}")

    def log(
        self,
        level: str,
        message: Any,
        props: Optional[Mapping[str, Any]] = None,
        *,
        timestamp: Timestamp = None,
        user_id: Any = None,
        device_id: Any = None,
        session_id: Any = None,
        anonymous_id: Any = None,
    ) -> None:
        """Queues a log line (debug, info, notice, warning, error, critical) with optional props; never raises."""
        try:
            event_object = self._base_fields(timestamp, user_id, device_id, session_id, anonymous_id)

            # An unknown level is kept by ingest as props.level, so it passes through.
            level_text = normalize_level(level)

            if level_text is not None:
                event_object["level"] = level_text

            event_object["message"] = _message_text(message)
            self._add_props(event_object, props)
            self._enqueue(event_object, is_log_line=True)
        except Exception as error:
            self._warn("internal", f"internal error, nothing was raised to the app: {error!r}")

    def set_enabled(self, is_enabled: bool) -> None:
        """Turns sending on or off; off is an opt-out that deletes every unsent line, from disk too."""
        try:
            self._is_enabled = bool(is_enabled) and self._check_key(should_warn=False)

            if not self._is_enabled:
                self._pipeline.clear()

                if self._pipeline.outbox is not None:
                    self._pipeline.outbox.acknowledge(0, [])
        except Exception as error:
            self._warn("internal", f"internal error, nothing was raised to the app: {error!r}")

    @property
    def pending_count(self) -> int:
        """Lines not yet acknowledged by a 202."""
        return self._pipeline.pending_count

    # The id, time and identity every line carries, set now so a retry counts once.
    def _base_fields(self, timestamp: Timestamp, user_id: Any, device_id: Any, session_id: Any, anonymous_id: Any) -> Dict[str, Any]:
        base_fields: Dict[str, Any] = {"event_id": new_event_id(), "ts": format_ts(timestamp)}
        identities = {"user_id": user_id, "device_id": device_id, "session_id": session_id, "anonymous_id": anonymous_id}

        for field_name, identity in identities.items():
            identity_text = clean_identity(identity)

            if identity_text is not None:
                base_fields[field_name] = identity_text

        return base_fields

    # Adds the call's props over the client's defaults, when there are any.
    def _add_props(self, event_object: Dict[str, Any], props: Optional[Mapping[str, Any]]) -> None:
        merged_props = merge_props(self._default_props, clean_props(props, self._warn, MAX_PROPS))

        if merged_props:
            event_object["props"] = merged_props

    # Encodes one line, applies the size cap, and queues it.
    def _enqueue(self, event_object: Dict[str, Any], is_log_line: bool) -> None:
        if not self._is_enabled or self._is_closed:
            return

        line = encode_line(event_object)
        max_line_bytes = self._pipeline.limits.max_request_bytes - 1

        # A line no request can hold: a log line is cut down, an event is skipped.
        if len(line) > max_line_bytes:
            if not is_log_line:
                self._warn("event_too_large", f"an event of {len(line)} bytes is over the request limit and was skipped")
                return

            self._warn("log_line_truncated", f"a log line of {len(line)} bytes was cut to fit one request")
            line = truncated_log_line(event_object, len(line), max_line_bytes)

            if len(line) > max_line_bytes:
                self._warn("event_too_large", f"a log line of {len(line)} bytes is over the request limit and was skipped")
                return

        is_batch_full = self._pipeline.enqueue([line])
        self._after_enqueue(is_batch_full)

    # Hook for the subclasses: start the sender, and wake it for a full batch.
    def _after_enqueue(self, is_batch_full: bool) -> None:
        raise NotImplementedError

    # What the drain loop does next, given an optional monotonic deadline.
    def _next_drain_step(self, deadline: Optional[float]) -> DrainStep:
        batch_entries = self._pipeline.next_batch()

        # Nothing left (or sending is off): the drain succeeded.
        if not batch_entries or not self._is_enabled:
            return ("stop", not batch_entries)

        # Paused: wait it out only when the pause ends before the deadline.
        wait_seconds = self._pipeline.wait_seconds()

        if wait_seconds > 0:
            if deadline is None or time.monotonic() + wait_seconds > deadline:
                return ("stop", False)

            return ("wait", wait_seconds)

        if deadline is not None and time.monotonic() >= deadline:
            return ("stop", False)

        return ("send", batch_entries)

    # Sends one batch and applies the answer; never raises.
    def _send_batch(self, batch_entries: List[Entry], deadline: Optional[float]) -> str:
        body = b"".join(entry.line + b"\n" for entry in batch_entries)

        # Near the deadline, the request may not outlive it.
        timeout_seconds = None if deadline is None else max(deadline - time.monotonic(), 0.5)

        try:
            answer = self._transport.send(body, timeout_seconds)
        except Exception as error:
            self._warn("network", f"send failed, will retry: {error!r}")
            return self._pipeline.apply_network_error()

        return self._pipeline.apply_answer(batch_entries, answer)

    # Sends on this thread until empty; true when nothing is left.
    def _drain_blocking(self, deadline: Optional[float]) -> bool:
        while True:
            step_kind, step_value = self._next_drain_step(deadline)

            if step_kind == "stop":
                return bool(step_value)

            if step_kind == "wait":
                time.sleep(cast(float, step_value))
                continue

            # Without a deadline a failure ends this pass; the pause decides the next.
            if self._send_batch(cast(List[Entry], step_value), deadline) == FAILED and deadline is None:
                return False

    # Opens the disk outbox when a directory is set; an unusable one falls back to memory.
    def _open_outbox(self, outbox_dir: Optional[str]) -> Optional[DiskOutbox]:
        if not outbox_dir:
            return None

        try:
            return DiskOutbox(outbox_dir)
        except DiskOutboxUnavailable as error:
            self._warn("outbox_open", f"the disk outbox is off, lines stay in memory: {error}")
            return None

    # Queues lines a dead process left behind in the outbox directory.
    def _adopt_orphaned_lines(self) -> None:
        if self._pipeline.outbox is None:
            return

        try:
            adopted_lines = self._pipeline.outbox.adopt_orphans()
        except OSError as error:
            self._warn("outbox_adopt", f"could not read leftover outbox files: {error}")
            return

        if adopted_lines and self._is_enabled:
            self._after_enqueue(self._pipeline.enqueue(adopted_lines))

    # Warns about keys that can't work; returns whether the key looks usable.
    def _check_key(self, should_warn: bool = True) -> bool:
        if self._ingest_key == "":
            if should_warn:
                self._warn("missing_key", "no ingest key (pass key= or set SIMPLEDATALOGS_KEY); nothing will be sent")
            return False

        if not self._ingest_key.startswith("sdl_") and should_warn:
            self._warn("key_shape", "the ingest key doesn't start with sdl_; check you copied the whole key")

        return True

    # Logs a warning once per kind, so a long outage never floods the app's logs.
    def _warn(self, kind: str, message: str) -> None:
        with self._warned_lock:
            if kind in self._warned_kinds:
                return

            self._warned_kinds.add(kind)

        try:
            self._logger.warning("[simpledatalogs] %s", message)
        except Exception:
            pass


class Client(_BaseClient):
    """The threaded client for WSGI apps, workers and scripts: a daemon thread flushes every few seconds."""

    def __init__(self, key: Optional[str] = None, **options: Any) -> None:
        """Takes the key and the options listed in the README; the thread starts with the first line."""
        self._send_lock = threading.Lock()
        self._wake_event = threading.Event()
        self._stop_event = threading.Event()
        self._worker: Optional[threading.Thread] = None
        self._worker_pid = 0
        super().__init__(key, **options)

        # A forked child (gunicorn, multiprocessing) gets fresh locks, its own thread and its own outbox file.
        if hasattr(os, "register_at_fork"):
            os.register_at_fork(after_in_child=_ForkReset(weakref.ref(self)))

    def flush(self, timeout: Optional[float] = 5.0) -> bool:
        """Sends what is queued, retrying within timeout seconds (None tries once); true when nothing is left."""
        try:
            deadline = None if timeout is None else time.monotonic() + max(timeout, 0.0)

            if not self._send_lock.acquire(timeout=-1 if timeout is None else max(timeout, 0.0)):
                return self.pending_count == 0

            try:
                return self._drain_blocking(deadline)
            finally:
                self._send_lock.release()
        except Exception as error:
            self._warn("internal", f"flush failed unexpectedly: {error!r}")
            return False

    def close(self, timeout: Optional[float] = 5.0) -> bool:
        """Stops the thread and flushes once more; later lines are ignored. True when nothing was left."""
        if self._is_closed:
            return self.pending_count == 0

        self._is_closed = True

        # Stop the thread first, so the last flush runs here alone.
        self._stop_event.set()
        self._wake_event.set()

        if self._worker is not None and self._worker.is_alive() and self._worker is not threading.current_thread():
            self._worker.join(timeout=1.0 if timeout is None else min(max(timeout, 0.0), 1.0))

        is_delivered = self.flush(timeout)

        # Unsent lines stay in the outbox file for the next process.
        if self._pipeline.outbox is not None:
            self._pipeline.outbox.close()

        return is_delivered

    def __enter__(self) -> "Client":
        """Returns the client, so `with Client(...) as sdl:` closes it at the end."""
        return self

    def __exit__(self, *exception_info: Any) -> None:
        """Closes the client, flushing what is left."""
        self.close()

    # Starts the thread on the first line, and wakes it for a full batch.
    def _after_enqueue(self, is_batch_full: bool) -> None:
        self._ensure_worker()

        if is_batch_full:
            self._wake_event.set()

    # Starts the sender thread, again in a forked child whose parent's thread didn't come along.
    def _ensure_worker(self) -> None:
        if self._is_closed or (self._worker is not None and self._worker_pid == os.getpid() and self._worker.is_alive()):
            return

        self._worker_pid = os.getpid()
        self._worker = threading.Thread(target=self._run_worker, name="simpledatalogs-flush", daemon=True)
        self._worker.start()

    # Sleeps until the interval passes, a pause ends or a full batch waits, then drains.
    def _run_worker(self) -> None:
        while not self._stop_event.is_set():
            try:
                pause_seconds = self._pipeline.wait_seconds()
                self._wake_event.wait(timeout=pause_seconds if pause_seconds > 0 else self._flush_interval_seconds)
                self._wake_event.clear()

                if self._stop_event.is_set():
                    return

                with self._send_lock:
                    self._drain_blocking(deadline=None)
            except Exception as error:
                self._warn("worker", f"the flush thread hit an error and carries on: {error!r}")
                self._stop_event.wait(self._flush_interval_seconds)

    # Resets the threading state in a forked child; the parent keeps sending its own lines.
    def _reset_after_fork(self) -> None:
        self._send_lock = threading.Lock()
        self._wake_event = threading.Event()
        self._stop_event = threading.Event()
        self._warned_lock = threading.Lock()
        self._worker = None
        self._pipeline.lock = threading.Lock()
        self._pipeline.clear()

        if self._pipeline.outbox is not None:
            try:
                self._pipeline.outbox.reopen_after_fork()
            except DiskOutboxUnavailable as error:
                self._warn("outbox_open", f"the disk outbox is off in this process: {error}")
                self._pipeline.outbox = None


class AsyncClient(_BaseClient):
    """The asyncio client for FastAPI, Starlette and aiohttp: a task flushes, sends run in a worker thread."""

    def __init__(self, key: Optional[str] = None, **options: Any) -> None:
        """Takes the same options as Client; the task starts with the first line tracked inside a running loop."""
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._task: Optional["asyncio.Task[None]"] = None
        self._wake_event: Optional[asyncio.Event] = None
        self._send_lock: Optional[asyncio.Lock] = None
        super().__init__(key, **options)

    async def flush(self, timeout: Optional[float] = 5.0) -> bool:
        """Sends what is queued, retrying within timeout seconds (None tries once); true when nothing is left."""
        try:
            self._bind_to_running_loop()
            deadline = None if timeout is None else time.monotonic() + max(timeout, 0.0)

            async with self._send_lock:  # type: ignore[union-attr]
                return await self._drain_async(deadline)
        except Exception as error:
            self._warn("internal", f"flush failed unexpectedly: {error!r}")
            return False

    async def aclose(self, timeout: Optional[float] = 5.0) -> bool:
        """Stops the task and flushes once more; later lines are ignored. True when nothing was left."""
        if self._is_closed:
            return self.pending_count == 0

        self._is_closed = True

        # Stop the background task first, so the last flush runs here alone.
        if self._task is not None and not self._task.done():
            self._task.cancel()

            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass

        is_delivered = await self.flush(timeout)

        if self._pipeline.outbox is not None:
            self._pipeline.outbox.close()

        return is_delivered

    async def __aenter__(self) -> "AsyncClient":
        """Returns the client, so `async with AsyncClient(...) as sdl:` closes it at the end."""
        self._bind_to_running_loop()
        return self

    async def __aexit__(self, *exception_info: Any) -> None:
        """Closes the client, flushing what is left."""
        await self.aclose()

    # Starts the task when a loop is running here, and wakes it for a full batch.
    def _after_enqueue(self, is_batch_full: bool) -> None:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            # Another thread (or no loop yet): nudge the loop we know, if any.
            if is_batch_full and self._loop is not None and self._wake_event is not None and not self._loop.is_closed():
                self._loop.call_soon_threadsafe(self._wake_event.set)
            return

        self._bind_to_running_loop()

        if (self._task is None or self._task.done()) and not self._is_closed:
            self._task = self._loop.create_task(self._run())  # type: ignore[union-attr]

        if is_batch_full:
            self._wake_event.set()  # type: ignore[union-attr]

    # Creates the loop-bound event and lock on first use inside the running loop.
    def _bind_to_running_loop(self) -> None:
        running_loop = asyncio.get_running_loop()

        if self._loop is running_loop:
            return

        self._loop = running_loop
        self._wake_event = asyncio.Event()
        self._send_lock = asyncio.Lock()
        self._task = None

    # Sleeps until the interval passes, a pause ends or a full batch waits, then drains.
    async def _run(self) -> None:
        while not self._is_closed:
            try:
                pause_seconds = self._pipeline.wait_seconds()

                try:
                    await asyncio.wait_for(self._wake_event.wait(), timeout=pause_seconds if pause_seconds > 0 else self._flush_interval_seconds)  # type: ignore[union-attr]
                except asyncio.TimeoutError:
                    pass

                self._wake_event.clear()  # type: ignore[union-attr]

                async with self._send_lock:  # type: ignore[union-attr]
                    await self._drain_async(deadline=None)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self._warn("worker", f"the flush task hit an error and carries on: {error!r}")
                await asyncio.sleep(self._flush_interval_seconds)

    # Sends from a worker thread until empty; true when nothing is left.
    async def _drain_async(self, deadline: Optional[float]) -> bool:
        running_loop = asyncio.get_running_loop()

        while True:
            step_kind, step_value = self._next_drain_step(deadline)

            if step_kind == "stop":
                return bool(step_value)

            if step_kind == "wait":
                await asyncio.sleep(cast(float, step_value))
                continue

            # The blocking HTTP call runs in the default executor, so the loop never waits on the network.
            outcome = await running_loop.run_in_executor(None, self._send_batch, cast(List[Entry], step_value), deadline)

            if outcome == FAILED and deadline is None:
                return False


class _ForkReset:
    """Calls a client's fork reset in the child, if the client still exists."""

    def __init__(self, client_reference: "weakref.ReferenceType[Client]") -> None:
        """Keeps a weak reference, so registering never keeps a client alive."""
        self.client_reference = client_reference

    def __call__(self) -> None:
        """Resets the client in the forked child."""
        client = self.client_reference()

        if client is not None:
            client._reset_after_fork()


# Flushes a client that was never closed when the interpreter exits.
def _close_client_at_exit(client_reference: "weakref.ReferenceType[_BaseClient]") -> None:
    client = client_reference()

    if client is None or client._is_closed:
        return

    try:
        # A sync client closes as usual; an async one drains on this thread, since its loop is gone.
        if isinstance(client, Client):
            client.close(client._exit_timeout_seconds)
            return

        client._is_closed = True
        client._drain_blocking(time.monotonic() + client._exit_timeout_seconds)

        if client._pipeline.outbox is not None:
            client._pipeline.outbox.close()
    except Exception:
        pass


# A log message as text; an exception reads as "Type: message".
def _message_text(message: Any) -> str:
    if isinstance(message, BaseException):
        return f"{type(message).__name__}: {message}"

    return "" if message is None else str(message)
