# Aqute

**A**sync **QU**eue **T**ask **E**ngine is an asyncio worker pool with configurable
retries, rate limits, and streaming results for independent I/O-bound jobs.
It requires Python 3.11 or newer and has no runtime dependencies.

Aqute runs in one process; pending work and results do not survive process
termination. CPU-heavy or blocking handlers block the event loop; Aqute does not
offload them to threads or processes.

Use it for API ingestion and backfills, infrastructure automation, or independent
remote inference and evaluation requests. Your application owns retry safety,
checkpoints, token budgets, and provider policy. See
[when to choose Aqute](https://github.com/insomnes/aqute/blob/main/docs/index.md#when-to-choose-aqute)
for a short comparison with plain asyncio and aiometer.

## Quickstart

```bash
pip install aqute
```

Pass your async handler to `Aqute`. Inside an async function, collect its results:

```python
results = await Aqute(handle, workers_count=4).process_all(range(10))
```

See the complete runnable [quickstart](https://github.com/insomnes/aqute/blob/main/examples/quickstart.py) for imports, a
handler, error handling, and `asyncio.run()`. `process_all()` returns an awaited
list in input order. `iter_results()` yields terminal results in completion order.
Both accept synchronous and asynchronous input. Inspect each task's `error` or
`success`; `None` can be a valid result.

Both helpers accept `submission_batch_size` (default `1`). Larger batches can
improve throughput for small tasks at the cost of result latency. The default
preserves per-item cooperative yielding. Handlers still receive one item per call;
configure worker concurrency and queue limits separately.

## Documentation and examples

[Usage](https://github.com/insomnes/aqute/blob/main/docs/usage.md) covers bounded buffering, iterator cleanup, rate limits,
retries, shutdown, counters, and deprecated method names. Full examples cover
[streaming](https://github.com/insomnes/aqute/blob/main/examples/streaming.py), a shared [HTTP client](https://github.com/insomnes/aqute/blob/main/examples/http_client.py),
and [service shutdown](https://github.com/insomnes/aqute/blob/main/examples/service_shutdown.py).

The [documentation source](https://github.com/insomnes/aqute/blob/main/docs/index.md) includes code directly from these
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

`make check` runs Ruff, ty, pytest with coverage, and the strict documentation build.
See [development](https://github.com/insomnes/aqute/blob/main/docs/development.md) for example commands and release behavior.
CI tests Python 3.11 through 3.14. See [LICENSE](https://github.com/insomnes/aqute/blob/main/LICENSE).

The measured coverage badge, XML, JSON, and HTML report are available in the
`coverage-python-3.11` artifact of each successful [CI run](https://github.com/insomnes/aqute/actions/workflows/ci.yml).
Run `make coverage` to generate the same files locally. See
[coverage reporting](https://github.com/insomnes/aqute/blob/main/docs/development.md#coverage) for the measurement scope.
