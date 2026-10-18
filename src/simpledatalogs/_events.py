"""Builds ingest API event objects: the id and time are set once, and props keep only what the API accepts."""

from __future__ import annotations

import enum
import json
import math
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Mapping, Optional, Union

# The ingest API takes at most 100 props per event.
MAX_PROPS = 100

# The ingest API's levels, in rising severity.
LEVELS = ("debug", "info", "notice", "warning", "error", "critical")

# Common spellings that map onto the ingest API's levels.
LEVEL_ALIASES = {
    "trace": "debug",
    "information": "info",
    "warn": "warning",
    "err": "error",
    "crit": "critical",
    "fatal": "critical",
    "alert": "critical",
    "emergency": "critical",
}

# The identity fields an event may carry.
IDENTITY_FIELDS = ("user_id", "device_id", "session_id", "anonymous_id", "install_id")

# The most of a message an oversized log line keeps.
TRUNCATED_MESSAGE_BYTES = 8192

# A point in time: a datetime, epoch seconds, or None for now.
Timestamp = Union[datetime, float, int, None]

# Reports a problem once per kind: warn(kind, message).
WarnCallback = Callable[[str, str], None]


def new_event_id() -> str:
    """A random UUID for event_id, so a retried copy is deduplicated."""
    return str(uuid.uuid4())


def format_ts(timestamp: Timestamp = None) -> str:
    """An ISO 8601 time with offset; now when none is given, and naive datetimes read as local time."""
    try:
        # A datetime keeps its moment; a naive one is local time, as Python treats it.
        if isinstance(timestamp, datetime):
            return timestamp.astimezone(timezone.utc).isoformat(timespec="milliseconds")

        # A finite epoch number is seconds.
        if isinstance(timestamp, (int, float)) and not isinstance(timestamp, bool) and math.isfinite(timestamp):
            return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat(timespec="milliseconds")
    except (OverflowError, OSError, ValueError):
        pass

    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def clean_identity(identity: Any) -> Optional[str]:
    """A trimmed identity string, or None for anything empty or not a string or number."""
    if isinstance(identity, bool) or not isinstance(identity, (str, int)):
        return None

    identity_text = str(identity).strip()

    return identity_text or None


def normalize_level(level: Any) -> Optional[str]:
    """The ingest API level for a level name; unknown names pass through lowercased, as ingest keeps them."""
    if not isinstance(level, str):
        return None

    level_text = level.strip().lower()

    if level_text == "":
        return None

    return LEVEL_ALIASES.get(level_text, level_text)


def scalar_prop_value(prop_value: Any) -> Any:
    """The value as a string, finite number or boolean, or None when it can't be one."""
    # Enums are common in apps; their value is what a chart shows.
    if isinstance(prop_value, enum.Enum):
        prop_value = prop_value.value

    if isinstance(prop_value, (str, bool, int)):
        return prop_value

    if isinstance(prop_value, float) and math.isfinite(prop_value):
        return prop_value

    return None


def clean_props(props: Any, warn: WarnCallback, room: int = MAX_PROPS) -> Dict[str, Any]:
    """Keeps string, finite number and boolean props, up to room of them, warning once about the rest."""
    if props is None:
        return {}

    if not isinstance(props, Mapping):
        warn("props_shape", "props must be a mapping; they were skipped")
        return {}

    kept_props: Dict[str, Any] = {}

    for prop_name, prop_value in props.items():
        # Skip values ingest can't chart.
        kept_value = scalar_prop_value(prop_value)

        if kept_value is None:
            warn("prop_value", f"prop {prop_name!r} is not a string, finite number or boolean; it was skipped")
            continue

        # Stop at the API's prop limit.
        if len(kept_props) >= room:
            warn("prop_count", f"an event had more than {room} props; the rest were skipped")
            break

        kept_props[str(prop_name)] = kept_value

    return kept_props


def merge_props(default_props: Mapping[str, Any], call_props: Mapping[str, Any]) -> Dict[str, Any]:
    """The client's default props under the call's own, within the API's prop limit."""
    merged_props = dict(call_props)

    for prop_name, prop_value in default_props.items():
        if prop_name not in merged_props and len(merged_props) < MAX_PROPS:
            merged_props[prop_name] = prop_value

    return merged_props


def encode_line(event_object: Mapping[str, Any]) -> bytes:
    """One NDJSON line as UTF-8 bytes; it never fails, so unencodable parts become text."""
    line_text = json.dumps(event_object, ensure_ascii=False, separators=(",", ":"), default=str)

    return line_text.encode("utf-8", "replace")


def truncated_log_line(event_object: Dict[str, Any], dropped_bytes: int, max_line_bytes: int) -> bytes:
    """An oversized log line rebuilt with a cut message and no props, saying how much was dropped."""
    # Leave room for the id, time, level and identity around the message.
    kept_message_bytes = max(min(TRUNCATED_MESSAGE_BYTES, max_line_bytes - 1024), 0)
    message_bytes = str(event_object.get("message", "")).encode("utf-8", "replace")
    cut_message = message_bytes[:kept_message_bytes].decode("utf-8", "ignore")

    smaller_object = {key: value for key, value in event_object.items() if key not in ("message", "props")}
    smaller_object["message"] = cut_message
    smaller_object["props"] = {"_truncated": True, "_dropped_bytes": dropped_bytes}

    return encode_line(smaller_object)
