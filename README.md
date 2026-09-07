# Aqute

**A**sync **QU**eue **T**ask **E**ngine processes coroutine calls with a worker pool,
optional rate limits, and retries. It requires Python 3.11 or newer and has no
runtime dependencies. It runs in one process; pending work and results do not
survive process termination.

## Quickstart

```bash
pip install aqute
```

Pass your async handler to `Aqute`. Inside an async function, collect its results:

```python
results = await Aqute(handle, workers_count=4).process_all(range(10))
```

See the complete runnable [quickstart](examples/quickstart.py) for imports, a
handler, error handling, and `asyncio.run()`. `process_all()` returns an awaited
list in input order. `iter_results()` yields terminal results in completion order.
Both accept synchronous and asynchronous input. Inspect each task's `error` or
`success`; `None` can be a valid result.

## Documentation and examples

[Usage](docs/usage.md) covers bounded buffering, iterator cleanup, rate limits,
retries, shutdown, counters, and deprecated method names. Full examples cover
[streaming](examples/streaming.py), a shared [HTTP client](examples/http_client.py),
and [service shutdown](examples/service_shutdown.py).

The [documentation source](docs/index.md) includes code directly from these
runnable files when built. To build and view the site locally:

```bash
uv sync --locked
make docs
uv run --locked mkdocs serve
```

## Development

Install [uv](https://docs.astral.sh/uv/getting-started/installation/), then run:

```bash
uv sync --locked
make check
make build
uv run --locked python -m examples.quickstart
```

`make check` runs Ruff, ty, pytest, and the strict documentation build.
See [development](docs/development.md) for example commands and release behavior.
CI tests Python 3.11 through 3.14. See [LICENSE](LICENSE).
