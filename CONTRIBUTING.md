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

## Audit the dependencies and check the types

```sh
docker run --rm -v "$PWD":/w -w /w python:3.13-slim sh -c 'pip install -q -e ".[test]" pip-audit mypy && pip-audit && mypy src'
```

## Rules the code follows

- The package has no runtime dependencies; keep it that way.
- Public calls never raise into the host app and never wait on the network.
- Behaviour follows the [sender rules](https://simpledatalogs.com/docs/api/sender-rules); a change there comes with a test against the fake ingest.
- Every function has a one-line docstring or comment, and names say what they hold.

## Releases

Maintainers bump `src/simpledatalogs/_version.py` and `CHANGELOG.md`, run the tests, then build and upload from their own machine with a PyPI API token (nothing runs on GitHub):

```sh
docker run --rm -it -v "$PWD":/w -w /w python:3.13-slim sh -c 'pip install --quiet build twine && python -m build && twine upload dist/*'
```

Then tag the same version (e.g. `0.1.0`) and push it.

## Security

Report vulnerabilities privately, as [SECURITY.md](SECURITY.md) describes.
