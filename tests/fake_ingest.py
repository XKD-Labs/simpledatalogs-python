"""A fake ingest server for tests: answers from a script and records every request."""

from __future__ import annotations

import gzip
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional, Tuple


class RecordedRequest:
    """One request as ingest saw it: path, headers, the body as sent, and its decoded lines."""

    def __init__(self, path: str, headers: Dict[str, str], raw_body: bytes) -> None:
        """Keeps the request and decompresses a gzip body."""
        self.path = path
        self.headers = headers
        self.raw_body = raw_body
        self.body = gzip.decompress(raw_body) if headers.get("content-encoding") == "gzip" else raw_body

    @property
    def lines(self) -> List[Dict[str, Any]]:
        """The NDJSON body's objects."""
        return [json.loads(line) for line in self.body.split(b"\n") if line.strip()]


class FakeIngest:
    """Runs on a free local port; each answer is (status, headers), and the last one repeats."""

    def __init__(self, answers: Optional[List[Tuple[int, Dict[str, str]]]] = None) -> None:
        """Starts the server in a daemon thread."""
        self.answers = list(answers or [(202, {})])
        self.requests: List[RecordedRequest] = []
        self.lock = threading.Lock()
        self.received = threading.Condition(self.lock)
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler_class())
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def endpoint(self) -> str:
        """The base URL to pass as endpoint=."""
        return f"http://127.0.0.1:{self.server.server_address[1]}"

    @property
    def accepted_lines(self) -> List[Dict[str, Any]]:
        """Every line of every request answered 202."""
        with self.lock:
            return [line for request, status in zip(self.requests, self._statuses()) if status == 202 for line in request.lines]

    def wait_for_requests(self, count: int, timeout: float = 5.0) -> bool:
        """Blocks until at least count requests arrived."""
        with self.received:
            return self.received.wait_for(lambda: len(self.requests) >= count, timeout=timeout)

    def close(self) -> None:
        """Stops the server."""
        self.server.shutdown()
        self.server.server_close()

    # The status each recorded request got, in order.
    def _statuses(self) -> List[int]:
        return [self.answers[min(index, len(self.answers) - 1)][0] for index in range(len(self.requests))]

    # Builds the request handler bound to this fake.
    def _handler_class(self) -> type:
        fake = self

        class Handler(BaseHTTPRequestHandler):
            """Records the POST and answers the next scripted status."""

            def do_POST(self) -> None:
                """Reads the body, records it, answers."""
                body_length = int(self.headers.get("Content-Length", "0"))
                raw_body = self.rfile.read(body_length)
                headers = {name.lower(): value for name, value in self.headers.items()}

                with fake.received:
                    answer_index = min(len(fake.requests), len(fake.answers) - 1)
                    fake.requests.append(RecordedRequest(self.path, headers, raw_body))
                    status, answer_headers = fake.answers[answer_index]
                    fake.received.notify_all()

                answer_body = b'{"accepted":"x"}' if status == 202 else b'{"error":"scripted"}'
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(answer_body)))

                for header_name, header_value in answer_headers.items():
                    self.send_header(header_name, header_value)

                self.end_headers()
                self.wfile.write(answer_body)

            def log_message(self, *args: Any) -> None:
                """Keeps test output quiet."""

        return Handler
