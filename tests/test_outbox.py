"""The disk outbox: lines survive a restart, a dead process's file is adopted, acknowledged lines go."""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any, Callable

import pytest

from fake_ingest import FakeIngest
from simpledatalogs._outbox import DiskOutbox


def outbox_files(directory: Path) -> list:
    """The outbox files in a directory."""
    return sorted(path.name for path in directory.iterdir() if path.name.endswith(".ndjson"))


def test_unsent_lines_are_sent_by_the_next_process(tmp_path: Path, fake_ingest: FakeIngest, make_client: Callable[..., Any]) -> None:
    fake_ingest.answers = [(503, {"Retry-After": "3600"}), (202, {})]
    first_client = make_client(outbox_dir=str(tmp_path))
    first_client.track("survives_restart")
    first_client.flush(timeout=None)
    first_client.close(timeout=0)

    assert len(outbox_files(tmp_path)) == 1

    second_client = make_client(outbox_dir=str(tmp_path))
    assert second_client.flush() is True

    assert [line["event"] for line in fake_ingest.accepted_lines] == ["survives_restart"]
    assert fake_ingest.requests[0].lines == fake_ingest.requests[1].lines


def test_a_killed_process_leaves_its_lines_for_adoption(tmp_path: Path, fake_ingest: FakeIngest, make_client: Callable[..., Any]) -> None:
    source_dir = Path(__file__).resolve().parent.parent / "src"
    crashing_script = textwrap.dedent(
        f"""
        import os, sys
        sys.path.insert(0, {str(source_dir)!r})
        import simpledatalogs
        client = simpledatalogs.Client("sdl_sec_test", endpoint="http://127.0.0.1:9", outbox_dir={str(tmp_path)!r}, flush_interval_seconds=3600)
        client.track("before_crash", {{"n": 1}})
        client.log("error", "also kept")
        os._exit(1)
        """
    )
    subprocess.run([sys.executable, "-c", crashing_script], check=False, timeout=30)

    client = make_client(outbox_dir=str(tmp_path))
    assert client.flush() is True

    assert [line.get("event", line.get("message")) for line in fake_ingest.accepted_lines] == ["before_crash", "also kept"]
    assert len(outbox_files(tmp_path)) <= 1


def test_a_live_process_file_is_never_adopted(tmp_path: Path) -> None:
    living_outbox = DiskOutbox(str(tmp_path))
    living_outbox.append([b'{"event":"mine"}'])

    other_outbox = DiskOutbox(str(tmp_path))

    assert other_outbox.adopt_orphans() == []
    assert len(outbox_files(tmp_path)) == 2

    living_outbox.close()
    other_outbox.close()


def test_acknowledged_lines_leave_the_file(tmp_path: Path, fake_ingest: FakeIngest, make_client: Callable[..., Any]) -> None:
    client = make_client(outbox_dir=str(tmp_path))
    client.track("acknowledged")

    assert client.flush() is True

    own_file = tmp_path / outbox_files(tmp_path)[0]
    assert own_file.read_bytes() == b""

    client.close()
    assert outbox_files(tmp_path) == []


def test_half_written_lines_are_skipped_on_adoption(tmp_path: Path) -> None:
    (tmp_path / "orphan.ndjson").write_bytes(b'{"event":"whole"}\n{"event":"cut')

    outbox = DiskOutbox(str(tmp_path))

    assert outbox.adopt_orphans() == [b'{"event":"whole"}']
    outbox.close()


def test_an_unwritable_directory_falls_back_to_memory(tmp_path: Path, fake_ingest: FakeIngest, make_client: Callable[..., Any]) -> None:
    blocking_file = tmp_path / "not_a_directory"
    blocking_file.write_text("x")

    client = make_client(outbox_dir=str(blocking_file / "outbox"))
    client.track("in_memory")

    assert client._pipeline.outbox is None
    assert client.flush() is True


def test_compaction_moves_the_rest_to_a_fresh_file(tmp_path: Path) -> None:
    outbox = DiskOutbox(str(tmp_path))
    big_line = b'{"event":"' + b"x" * 600_000 + b'"}'
    outbox.append([big_line, big_line, b'{"event":"rest"}'])
    first_name = outbox_files(tmp_path)

    outbox.acknowledge(len(big_line) * 2 + 2, [b'{"event":"rest"}'])

    assert outbox_files(tmp_path) != first_name
    assert (tmp_path / outbox_files(tmp_path)[0]).read_bytes() == b'{"event":"rest"}\n'
    outbox.close()


@pytest.mark.filterwarnings("ignore:This process .* is multi-threaded:DeprecationWarning")
def test_a_forked_child_writes_its_own_file(tmp_path: Path, make_client: Callable[..., Any]) -> None:
    if not hasattr(os, "fork"):
        return

    client = make_client(outbox_dir=str(tmp_path), flush_interval_seconds=3600, endpoint="http://127.0.0.1:9")
    client.track("parent")

    child_pid = os.fork()

    if child_pid == 0:
        client.track("child")
        os._exit(0 if client.pending_count == 1 else 1)

    _, child_status = os.waitpid(child_pid, 0)

    assert os.WIFEXITED(child_status) and os.WEXITSTATUS(child_status) == 0
    assert len(outbox_files(tmp_path)) == 2
    assert client.pending_count == 1
