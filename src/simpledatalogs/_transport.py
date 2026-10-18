"""One POST to the ingest API with the standard library: gzip, the key, the User-Agent, and no redirects."""

from __future__ import annotations

import gzip
import urllib.error
import urllib.request
from typing import NamedTuple, Optional

from ._retry import parse_retry_after
from ._version import __version__

# Bodies from this size are gzipped; smaller ones gain nothing.
GZIP_MIN_BYTES = 1024

# The most of an answer body read before the socket is closed; ingest's answers are tiny.
MAX_ANSWER_BYTES = 65536

# Names this SDK and its version in every request.
USER_AGENT = f"simpledatalogs-python/{__version__}"


class Answer(NamedTuple):
    """The status ingest answered and its Retry-After in seconds, if any."""

    status: int
    retry_after_seconds: Optional[float]


class _NoRedirects(urllib.request.HTTPRedirectHandler):
    """Refuses redirects, so the key never follows a POST elsewhere and a 3xx counts as not stored."""

    # Returns no new request, so urllib raises the 3xx as an HTTPError.
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        return None


class UrllibTransport:
    """Sends one NDJSON body to the ingest URL and returns the answer; network errors raise."""

    def __init__(self, ingest_url: str, ingest_key: str, timeout_seconds: float, should_gzip: bool = True) -> None:
        """Keeps the URL, the key, the timeout and whether to gzip."""
        self.ingest_url = ingest_url
        self.timeout_seconds = timeout_seconds
        self.should_gzip = should_gzip
        self._ingest_key = ingest_key
        self._opener = urllib.request.build_opener(_NoRedirects)

    def send(self, body: bytes, timeout_seconds: Optional[float] = None) -> Answer:
        """POSTs the body; any HTTP status is an Answer, a timeout or network failure raises OSError."""
        headers = {
            "Authorization": f"Bearer {self._ingest_key}",
            "Content-Type": "application/x-ndjson",
            "User-Agent": USER_AGENT,
        }

        # Gzip counts its compressed bytes against the 512 KB limit, so larger bodies shrink.
        if self.should_gzip and len(body) >= GZIP_MIN_BYTES:
            body = gzip.compress(body, compresslevel=6)
            headers["Content-Encoding"] = "gzip"

        request = urllib.request.Request(self.ingest_url, data=body, headers=headers, method="POST")

        try:
            # A caller's shorter timeout (a flush near its deadline) wins over the configured one.
            request_timeout_seconds = self.timeout_seconds if timeout_seconds is None else min(self.timeout_seconds, timeout_seconds)

            with self._opener.open(request, timeout=request_timeout_seconds) as response:
                # Read the small answer, so the connection closes cleanly.
                response.read(MAX_ANSWER_BYTES)
                return Answer(response.status, parse_retry_after(response.headers.get("Retry-After")))
        except urllib.error.HTTPError as http_error:
            # A non-2xx status is still an answer; its Retry-After decides the wait.
            try:
                retry_after_header = http_error.headers.get("Retry-After") if http_error.headers else None
                return Answer(http_error.code, parse_retry_after(retry_after_header))
            finally:
                http_error.close()
