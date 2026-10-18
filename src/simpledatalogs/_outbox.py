"""The optional disk outbox: one locked NDJSON file per process, adopted by the next process if its owner dies."""

from __future__ import annotations

import json
import os
import uuid
from typing import BinaryIO, Iterable, List, Optional

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows has no fcntl
    fcntl = None  # type: ignore[assignment]

# Outbox files end in this; a file still being created has another suffix, so nobody adopts it half-made.
OUTBOX_SUFFIX = ".ndjson"

# Compaction waits until this many acknowledged bytes have piled up at the front of the file.
COMPACT_MIN_BYTES = 1_000_000


class DiskOutboxUnavailable(Exception):
    """The disk outbox can't run here (no file locks, or the directory isn't writable)."""


class DiskOutbox:
    """Appends every line as it is tracked, and forgets lines once ingest acknowledged them."""

    def __init__(self, directory: str) -> None:
        """Creates the directory and this process's own locked file."""
        if fcntl is None:
            raise DiskOutboxUnavailable("file locks (fcntl) are not available on this platform")

        try:
            os.makedirs(directory, mode=0o700, exist_ok=True)
        except OSError as error:
            raise DiskOutboxUnavailable(f"could not create {directory}: {error}") from error

        self.directory = directory
        self._file: Optional[BinaryIO] = None
        self._file_path = ""
        self._file_bytes = 0
        self._acknowledged_bytes = 0
        self._open_own_file()

    def adopt_orphans(self) -> List[bytes]:
        """Reads the lines of every outbox whose owner is gone, then deletes those files."""
        adopted_lines: List[bytes] = []

        for file_name in sorted(os.listdir(self.directory)):
            orphan_path = os.path.join(self.directory, file_name)

            if not file_name.endswith(OUTBOX_SUFFIX) or orphan_path == self._file_path:
                continue

            adopted_lines.extend(self._adopt_if_orphaned(orphan_path))

        return adopted_lines

    def append(self, lines: Iterable[bytes]) -> None:
        """Appends lines to this process's file and flushes them to the OS."""
        if self._file is None:
            return

        chunk = b"".join(line + b"\n" for line in lines)
        self._file.write(chunk)
        self._file.flush()
        self._file_bytes += len(chunk)

    def acknowledge(self, acknowledged_bytes: int, remaining_lines: List[bytes]) -> None:
        """Notes lines done with; empties the file when nothing is left, and compacts it when mostly done."""
        if self._file is None:
            return

        self._acknowledged_bytes += acknowledged_bytes

        # Nothing pending: start the file over.
        if not remaining_lines:
            self._file.seek(0)
            self._file.truncate()
            self._file_bytes = 0
            self._acknowledged_bytes = 0
            return

        # Mostly acknowledged: move the rest into a fresh file, so a restart doesn't resend much.
        if self._acknowledged_bytes >= COMPACT_MIN_BYTES and self._acknowledged_bytes * 2 >= self._file_bytes:
            self._replace_own_file(remaining_lines)

    def reopen_after_fork(self) -> None:
        """Gives a forked child its own file; the parent keeps the inherited one and its lock."""
        if self._file is not None:
            # Closing (never unlocking) the inherited copy leaves the parent's lock in place.
            self._file.close()
            self._file = None

        self._open_own_file()

    def close(self) -> None:
        """Closes this process's file; an empty one is deleted, a non-empty one waits for adoption."""
        if self._file is None:
            return

        is_empty = self._file_bytes == 0
        own_file = self._file
        self._file = None

        if is_empty:
            _remove_quietly(self._file_path)

        own_file.close()

    # Creates a file under a temporary name, locks it, then renames it, so it is never adoptable unlocked.
    def _open_own_file(self) -> None:
        file_id = uuid.uuid4().hex
        creating_path = os.path.join(self.directory, f"{file_id}.creating")
        final_path = os.path.join(self.directory, f"{file_id}{OUTBOX_SUFFIX}")

        try:
            file_descriptor = os.open(creating_path, os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_APPEND, 0o600)
            fcntl.flock(file_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            os.rename(creating_path, final_path)
        except OSError as error:
            raise DiskOutboxUnavailable(f"could not create an outbox file in {self.directory}: {error}") from error

        self._file = os.fdopen(file_descriptor, "ab+")
        self._file_path = final_path
        self._file_bytes = 0
        self._acknowledged_bytes = 0

    # Writes the remaining lines to a new locked file, then drops the old one.
    def _replace_own_file(self, remaining_lines: List[bytes]) -> None:
        old_file = self._file
        old_path = self._file_path

        self._open_own_file()
        self.append(remaining_lines)

        _remove_quietly(old_path)

        if old_file is not None:
            old_file.close()

    # Takes an outbox file's lines when no process holds its lock; a held lock means its owner is alive.
    def _adopt_if_orphaned(self, orphan_path: str) -> List[bytes]:
        try:
            orphan_file = open(orphan_path, "rb")
        except OSError:
            return []

        with orphan_file:
            try:
                fcntl.flock(orphan_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                return []

            adopted_lines = [line for line in orphan_file.read().split(b"\n") if _is_complete_line(line)]
            _remove_quietly(orphan_path)

        return adopted_lines


# A line worth resending: a whole JSON object, not a blank or a half-written tail.
def _is_complete_line(line: bytes) -> bool:
    if line.strip() == b"":
        return False

    try:
        return isinstance(json.loads(line), dict)
    except ValueError:
        return False


# Deletes a file, ignoring one that is already gone.
def _remove_quietly(file_path: str) -> None:
    try:
        os.remove(file_path)
    except OSError:
        pass
