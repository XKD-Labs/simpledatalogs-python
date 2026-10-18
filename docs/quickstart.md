---
description: Send events and log lines from Django, Flask, FastAPI or any Python process to simpledatalogs in one minute.
---

# Python (Django, Flask and FastAPI)

## Install

```sh
pip install simpledatalogs
```

```sh
# .env
SIMPLEDATALOGS_KEY=sdl_sec_…
```

## Add it

```python
import simpledatalogs

sdl = simpledatalogs.init()

sdl.track("signup", {"plan": "pro"}, user_id=42)
```

A background thread sends every 5 seconds; whatever is left goes out when the process exits.

## Send your logs

```python
# Django: settings.py
import simpledatalogs

simpledatalogs.init()

LOGGING = {
    "version": 1,
    "handlers": {"simpledatalogs": {"class": "simpledatalogs.SimpleDataLogsHandler", "level": "WARNING"}},
    "root": {"handlers": ["simpledatalogs"], "level": "INFO"},
}
```

```python
# Flask
import logging

app.logger.addHandler(simpledatalogs.SimpleDataLogsHandler(level=logging.WARNING))
```

## FastAPI

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
```

## In the first minute

- Your `track()` events within 5 seconds.
- Warnings and errors on the **Logs** tab, with `exception.type`, `exception.message` and `stacktrace` for exceptions.

## Defaults

| Feature | Default |
|---|---|
| Sending | every 5 s, or at 50 lines; retried until a `202` |
| Queue | memory, at most 10,000 lines or 10 MB; `outbox_dir=` keeps unsent lines on disk |
| Exit | flushes for up to 5 s |
| Autocapture | none: only what you track and what your loggers emit at the handler's level |

Everything else is in the [Python SDK](https://github.com/XKD-Labs/simpledatalogs-python) README.
