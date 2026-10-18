# Changelog

All notable changes to the `simpledatalogs` Python package. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the package uses [Semantic Versioning](https://semver.org/).

## [0.1.0] - Unreleased

The first public release.

### Added

- `Client` (a background flush thread) and `AsyncClient` (an asyncio task, sends in a worker thread), plus `init()`, `track()`, `log()`, `flush()` and `close()` for a process-wide client.
- `SimpleDataLogsHandler`, a `logging.Handler` that ships records with their level, time, extras and exceptions.
- A bounded in-memory queue, and an optional disk outbox (`outbox_dir`) that survives crashes and is adopted by the next process.
- Batching, gzip, retries with full-jitter backoff, `Retry-After`, `401` pauses, `413` splitting and `415` drops, and a flush at interpreter exit.
- Fork safety for gunicorn and uWSGI workers.
