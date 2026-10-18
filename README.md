# simpledatalogs (Python)

Sends events and log lines from Python servers, workers and scripts to [simpledatalogs](https://simpledatalogs.com), following the [sender rules](https://simpledatalogs.com/docs/api/sender-rules).
Standard library only, typed, Python 3.9+.

## Install

```sh
pip install simpledatalogs
```

## Quick start

```python
import simpledatalogs

sdl = simpledatalogs.init()  # reads SIMPLEDATALOGS_KEY=sdl_sec_…

sdl.track("signup", {"plan": "pro"}, user_id=42)
sdl.log("error", "Payment failed", {"order_id": 7})
```

That's all: a background thread sends every 5 seconds, and whatever is left goes out when the process exits.
Use a **secret key** (`sdl_sec_…`) on servers; public keys are for apps and web pages.

## Configuration

`simpledatalogs.init(key=None, **options)` creates the process-wide client; `simpledatalogs.Client(key, **options)` creates one you hold yourself. Both take:

| Option | Default | What it does |
|---|---|---|
| `key` | `SIMPLEDATALOGS_KEY` | The ingest key. Without one the client is off and says so once |
| `endpoint` | `SIMPLEDATALOGS_ENDPOINT`, else `https://edge.simpledatalogs.com` | The ingest host; `/v1/ingest` is added |
| `flush_interval_seconds` | `5` | How often the background thread sends |
| `max_batch_events` | `50` | Lines per request |
| `max_request_bytes` | `256000` | Bytes per request before gzip; never above 512 KB |
| `max_queue_events` / `max_queue_bytes` | `10000` / `10000000` | The queue's caps; past them the oldest unsent lines are dropped, with a warning |
| `outbox_dir` | none | A directory for the disk outbox (below) |
| `default_props` | none | Props added to every line, under the call's own |
| `gzip` | `True` | Gzips bodies of 1 KB and more |
| `request_timeout_seconds` | `15` | Per request |
| `flush_at_exit` / `exit_timeout_seconds` | `True` / `5` | Flush at interpreter exit, for at most this long |
| `enabled` | `True` | `False` keeps the client silent (tests, local runs) |

The client never raises into your app. Problems are logged once each on the `simpledatalogs` logger.

## Events

```python
sdl.track("checkout", {"plan": "pro", "seats": 3, "trial": False}, user_id=user.id)
sdl.track("import_done", {"rows": 1200}, timestamp=finished_at)  # a datetime or epoch seconds
```

- Every line gets a UUID `event_id` and a `ts` when you call `track()` or `log()`, so a retry never counts twice.
- Props are strings, finite numbers or booleans (enums send their value); anything else is skipped with a warning. At most 100 props.
- Identity is per call: `user_id`, `device_id`, `session_id`, `anonymous_id`.
- `sdl.flush(timeout=5)` sends now and returns `True` when nothing is left; `sdl.close()` flushes and stops the thread.
- `sdl.set_enabled(False)` is an opt-out: it deletes every unsent line and ignores new ones until `set_enabled(True)`.

## Logs

`SimpleDataLogsHandler` is a `logging.Handler`. Each record becomes one log line with the record's level and time.

```python
import logging
import simpledatalogs

simpledatalogs.init()
logging.getLogger().addHandler(simpledatalogs.SimpleDataLogsHandler(level=logging.INFO))

logging.getLogger("payments").error("Payment failed", extra={"order_id": 7, "user_id": 42})
```

- Props: `logger`, scalar `extra=` values, and for exceptions `exception.type`, `exception.message` and `stacktrace` (the last 8 KB).
- `user_id`, `device_id`, `session_id` and `anonymous_id` in `extra=` fill the identity fields.
- Levels map to `debug`, `info`, `warning`, `error` and `critical`. Pass `include_extras=False` to send no extras.
- The handler only queues; it never waits on the network.

### Django

```python
# settings.py
import simpledatalogs

simpledatalogs.init()

LOGGING = {
    "version": 1,
    "handlers": {"simpledatalogs": {"class": "simpledatalogs.SimpleDataLogsHandler", "level": "WARNING"}},
    "root": {"handlers": ["simpledatalogs"], "level": "INFO"},
}
```

With gunicorn or uWSGI, each worker process gets its own sending thread after the fork; nothing else to do.

### Flask

```python
import logging
import simpledatalogs
from flask import Flask

sdl = simpledatalogs.init()
app = Flask(__name__)
app.logger.addHandler(simpledatalogs.SimpleDataLogsHandler(sdl, level=logging.WARNING))


@app.post("/signup")
def signup():
    sdl.track("signup", {"plan": "pro"})
    return {"ok": True}
```

### FastAPI (asyncio)

`AsyncClient` takes the same options. Its sends run in a worker thread, so the event loop never waits on the network.

```python
from contextlib import asynccontextmanager

import simpledatalogs
from fastapi import FastAPI

sdl = simpledatalogs.AsyncClient()


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await sdl.aclose()


app = FastAPI(lifespan=lifespan)


@app.post("/signup")
async def signup():
    sdl.track("signup", {"plan": "pro"})
    return {"ok": True}
```

`await sdl.flush()` sends now; `async with simpledatalogs.AsyncClient() as sdl:` closes it for you.

## Delivery

- **Batches:** every 5 s, or at once when 50 lines wait; each request stays under 256 KB before gzip, so far under the 512 KB limit.
- **Only a `202` counts as stored.** Everything else is retried with full-jitter backoff from 1 s to 5 min, never sooner than `Retry-After` (`429`, `503`).
- `401` keeps every line and re-checks the key after 1 minute, then every 10 minutes. `413` halves the batch (a single line too large is dropped); `415` drops the batch with one warning.
- **Exit:** at interpreter exit the client flushes for up to 5 s, waiting out short `Retry-After`s. Call `close()` yourself in workers that exit through `os._exit`.
- **Memory queue:** bounded by `max_queue_events` and `max_queue_bytes`; the oldest lines go first, with a warning.

### Disk outbox

```python
sdl = simpledatalogs.init(outbox_dir="/var/lib/myapp/simpledatalogs")
```

Each line is appended to a file as it is tracked and leaves it after a `202`, so a crash or a deploy loses nothing; the next process sends what is left.
Each process keeps its own locked file. A file whose process is gone is picked up by the next client that starts on the same directory.
The disk outbox needs POSIX file locks (Linux, macOS); elsewhere the client warns and keeps lines in memory.

## Privacy

- The SDK sends only what you pass: no IP addresses, hostnames, environment variables or request data are added.
- `extra=` values reach ingest as you logged them. Leave out personal data, or pass `include_extras=False`.
- An email address in `user_id` is stored as a keyed pseudonym unless the project's owner chose otherwise.
- `set_enabled(False)` deletes unsent lines, from disk too.

## Links

- Docs: https://simpledatalogs.com/docs
- Ingest API: https://simpledatalogs.com/docs/api/ingest
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Security: [SECURITY.md](SECURITY.md)
- Contributing: [CONTRIBUTING.md](CONTRIBUTING.md)
- License: [MIT](LICENSE)
