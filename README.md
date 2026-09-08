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
[when to choose Aqute](https://insomnes.github.io/aqute/#when-to-choose-aqute)
for a short comparison with plain asyncio and aiometer.

## Quickstart

This README and the linked documentation cover the 0.10.0 API
for Python 3.11+. Follow the
[installation instructions](https://insomnes.github.io/aqute/#installation)
before running these examples. For the 0.9.x maintenance API, use the
[0.9.3 quickstart](https://github.com/insomnes/aqute/blob/0.9.3/README.md#quickstart).

Pass your async handler to `Aqute`. Inside an async function, collect its results:

```python
results = await Aqute(handle, workers_count=4).process_all(range(10))
```

See the complete runnable [quickstart](https://github.com/insomnes/aqute/blob/main/examples/quickstart.py) for imports, a
handler, error handling, and `asyncio.run()`. `process_all()` returns an awaited
list in input order. For completion-order streaming, use
`async with engine.iter_results(items) as results` and iterate `results` inside the
context. Context exit awaits producer and worker cleanup, including after early
exit. Both helpers accept synchronous and asynchronous input. Inspect each task's
`error` or `success`; `None` can be a valid result.

Both helpers accept `submission_batch_size` (default `1`). Larger batches can
improve throughput for small tasks at the cost of result latency. The default
preserves per-item cooperative yielding. Handlers still receive one item per call;
configure worker concurrency and queue limits separately.

All APIs default each queue to `workers_count` items. Manual runs must consume
results concurrently, or explicitly choose unlimited queues with `0` for
[run-then-drain or pre-submission](https://insomnes.github.io/aqute/usage/#queue-default-migration).

## Bounded HTTP processing

The [runnable HTTP example](https://insomnes.github.io/aqute/http_client/) combines
a shared HTTPX client, four workers, an attempt-rate limiter, selected retries,
and incremental result consumption. Its managed stream uses finite queue defaults
and closes processing before the client. It logs each page and returns a count;
queue limits bound items, not response bytes or data retained by your application.

From a development checkout, run it without network access:

```bash
uv run --locked python -m examples.http_client
```

The example first compares throttling with and without a shared pause. It then
completes a transport retry and handles a terminal HTTP error in a separate run.
Its [source](https://github.com/insomnes/aqute/blob/main/examples/http_client.py)
also provides an explicit path for real requests. The HTTP page includes the
source and explains buffering overrides, retry safety, and its fail-fast policy.

## Documentation and examples

The [documentation](https://insomnes.github.io/aqute/) includes the complete quickstart.
[Usage](https://insomnes.github.io/aqute/usage/) covers bounded buffering, streaming cleanup, rate limits,
retries, shutdown, counters, and migration from 0.9.2. The
[changelog](https://github.com/insomnes/aqute/blob/main/CHANGELOG.md) collects the breaking changes and method renames. Full examples cover
[streaming](https://insomnes.github.io/aqute/streaming/), [bounded HTTP processing](https://insomnes.github.io/aqute/http_client/),
[manual result draining](https://insomnes.github.io/aqute/manual_drain/),
and [service shutdown](https://insomnes.github.io/aqute/service_shutdown/).

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
See [development](https://insomnes.github.io/aqute/development/) for example commands and release behavior.
CI tests Python 3.11 through 3.14. See [LICENSE](https://github.com/insomnes/aqute/blob/main/LICENSE).

The measured coverage badge, XML, JSON, and HTML report are available in the
`coverage-python-3.11` artifact of each successful [CI run](https://github.com/insomnes/aqute/actions/workflows/ci.yml).
Run `make coverage` to generate the same files locally. See
[coverage reporting](https://insomnes.github.io/aqute/development/#coverage) for the measurement scope.
