"""The retry policy: full-jitter backoff from 1 s to 5 min, with Retry-After as a floor."""

from __future__ import annotations

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Optional

# Backoff bounds for answers that weren't a 202, in seconds.
INITIAL_BACKOFF_SECONDS = 1.0
MAX_BACKOFF_SECONDS = 300.0

# No retry comes sooner than this, so a burst of failures never spins.
MIN_DELAY_SECONDS = 0.25

# The longest Retry-After honoured in one wait; over quota asks an hour.
MAX_RETRY_AFTER_SECONDS = 3600.0

# After a 401 the key is re-probed after a minute, then every 10 minutes.
INVALID_KEY_FIRST_PROBE_SECONDS = 60.0
INVALID_KEY_PROBE_SECONDS = 600.0


def retry_delay_seconds(failure_count: int, retry_after_seconds: Optional[float], jitter_fraction: float) -> float:
    """The wait before the next attempt: full jitter under 1 s * 2^(n-1), capped at 5 min, never under Retry-After."""
    # Cap the exponent so the doubling can never overflow.
    doubling_count = min(max(failure_count - 1, 0), 30)
    ceiling_seconds = min(MAX_BACKOFF_SECONDS, INITIAL_BACKOFF_SECONDS * 2**doubling_count)

    # Full jitter picks uniformly under the ceiling, then the floor and Retry-After apply.
    clamped_fraction = min(max(jitter_fraction, 0.0), 1.0)

    return honor_retry_after(max(MIN_DELAY_SECONDS, ceiling_seconds * clamped_fraction), retry_after_seconds)


def honor_retry_after(delay_seconds: float, retry_after_seconds: Optional[float]) -> float:
    """The delay, raised to Retry-After within a sane bound."""
    if retry_after_seconds is None:
        return delay_seconds

    return max(delay_seconds, min(retry_after_seconds, MAX_RETRY_AFTER_SECONDS))


def parse_retry_after(header_value: Optional[str], now: Optional[datetime] = None) -> Optional[float]:
    """Seconds from a Retry-After header (delay-seconds or an HTTP-date), or None when absent or unreadable."""
    trimmed_value = (header_value or "").strip()

    if trimmed_value == "":
        return None

    # delay-seconds is a bare non-negative integer.
    if trimmed_value.isdigit():
        return float(trimmed_value)

    # An HTTP-date in the past means retry now.
    try:
        retry_at = parsedate_to_datetime(trimmed_value)
    except (TypeError, ValueError, IndexError):
        return None

    if retry_at is None:
        return None

    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=timezone.utc)

    current_time = now or datetime.now(timezone.utc)

    return max(0.0, (retry_at - current_time).total_seconds())
