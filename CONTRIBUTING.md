# Contributing

Thanks for helping. Bug reports, fixes and docs improvements are welcome; for a larger change, open an issue first so we can agree on the shape.

## Run the tests

The tests need only Docker; nothing is installed on your machine.

```sh
docker run --rm -v "$PWD":/w -w /w python:3.13-slim sh -c 'pip install -q -e ".[test]" && python -m pytest -q'
```

Check the oldest supported Python too:

```sh
docker run --rm -v "$PWD":/w -w /w python:3.9-slim sh -c 'pip install -q -e ".[test]" && python -m pytest -q'
```

With a local virtualenv instead: `pip install -e ".[test]"`, then `python -m pytest -q`.

The tests start a fake ingest server on `127.0.0.1`; they never reach the internet.

## Audit the dependencies

```sh
docker run --rm -v "$PWD":/w -w /w python:3.13-slim sh -c 'pip install -q pip-audit && pip-audit .'
```

## Rules the code follows

- The package has no runtime dependencies; keep it that way.
- Public calls never raise into the host app and never wait on the network.
- Behaviour follows the [sender rules](https://simpledatalogs.com/docs/api/sender-rules); a change there comes with a test against the fake ingest.
- Every function has a one-line docstring or comment, and names say what they hold.

## Releases

Maintainers bump `src/simpledatalogs/_version.py` and `CHANGELOG.md`, then push a tag with the same version (e.g. `0.1.0`). The publish workflow tests, builds and uploads to PyPI with trusted publishing.

## Security

Report vulnerabilities privately, as [SECURITY.md](SECURITY.md) describes.
