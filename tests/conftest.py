"""Shared fixtures: a fake ingest server and a client factory that always closes."""

from __future__ import annotations

import os
import sys
from typing import Any, Callable, Iterator, List

import pytest

sys.path.insert(0, os.path.dirname(__file__))

from fake_ingest import FakeIngest  # noqa: E402

import simpledatalogs  # noqa: E402

# A key shaped like a real secret key, for tests only.
TEST_KEY = "sdl_sec_test_0000000000000000"


@pytest.fixture
def fake_ingest() -> Iterator[FakeIngest]:
    """A fake ingest that answers 202 until a test changes its answers."""
    server = FakeIngest()
    yield server
    server.close()


@pytest.fixture
def make_client(fake_ingest: FakeIngest) -> Iterator[Callable[..., simpledatalogs.Client]]:
    """Builds clients against the fake ingest, without exit hooks, and closes them after the test."""
    created_clients: List[simpledatalogs.Client] = []

    def build_client(**options: Any) -> simpledatalogs.Client:
        options.setdefault("endpoint", fake_ingest.endpoint)
        options.setdefault("flush_at_exit", False)
        client = simpledatalogs.Client(options.pop("key", TEST_KEY), **options)
        created_clients.append(client)
        return client

    yield build_client

    for client in created_clients:
        client.close(timeout=0)
