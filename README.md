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

## Installation

Aqute requires Python 3.11+. Install the version used by these examples:

```bash
python -m pip install aqute==0.10.0
```

## Quickstart

Save any example below as `quickstart.py` and run `python quickstart.py`.
The first three need only Aqute and the standard library. The HTTP example also
needs `httpx`.

The first three handlers use `await asyncio.sleep(0.1)` to simulate I/O
without blocking the event loop. The HTTP handler awaits real network I/O.

### 1. Collect a finite batch in input order

Start here when the full result list fits in memory. The handler receives
one input item per call; `workers_count=4` permits up to four concurrent handlers.

```python
"""Run a finite batch and receive results in input order."""

import asyncio
import logging

from aqute import Aqute


async def handle(value: int) -> int:
    await asyncio.sleep(0.1)  # Simulate asynchronous I/O.
    return value * 2


async def main() -> list[int]:
    tasks = await Aqute(handle, workers_count=4).process_all(range(10))
    values = [task.unwrap() for task in tasks]
    assert values == [value * 2 for value in range(10)]
    return values


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("Results: %s", asyncio.run(main()))
```

The result is `[0, 2, 4, 6, 8, 10, 12, 14, 16, 18]`. `process_all()` returns
completed task objects in input order. `task.unwrap()` returns the handler value
or raises its error. Here, unwrapping happens after the whole batch finishes;
it does not stop the batch on its first failure.

### 2. Record errors and continue

Inspect `task.error` when one bad item must not prevent you from using the
other results. This example uses local conversion to make a failure reproducible.

```python
"""Report each failed item and keep the successful results."""

import asyncio
import logging

from aqute import Aqute


async def parse_port(value: str) -> int:
    await asyncio.sleep(0.1)  # Simulate asynchronous I/O.
    return int(value)


async def main() -> None:
    tasks = await Aqute(parse_port, workers_count=2).process_all(
        ["443", "invalid", "8080"]
    )
    for task in tasks:
        if task.error is not None:
            logging.warning("Failed %r: %s", task.data, task.error)
        else:
            logging.info("Port: %s", task.unwrap())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
```

This logs ports `443` and `8080`, plus a failure for `"invalid"`. Handler
failures are stored on completed tasks; retries are disabled by default.
Check `error` or `success`, not whether `result` is `None`: a successful handler
can return `None`. Input-source and engine errors can still raise from a helper.

### 3. Consume a stream and stop early

Use `iter_results()` to consume results as they complete without collecting
the full output. This example takes an asynchronous input source and stops
after three results.

```python
"""Consume bounded streaming results and stop early with managed cleanup."""

import asyncio
import logging

from aqute import Aqute

logger = logging.getLogger(__name__)
RESULT_LIMIT = 3


async def main() -> int:
    async def source():
        try:
            for value in range(100):
                yield value
        finally:
            logger.info("Source closed")

    async def handle(value: int) -> int:
        await asyncio.sleep(0.1)  # Simulate asynchronous I/O.
        return value * 2

    engine = Aqute(handle, workers_count=3)
    completed = 0
    async with engine.iter_results(source()) as results:
        async for task in results:
            # Replace this log with application-owned processing or persistence.
            logger.info("Result for %s: %s", task.data, task.unwrap())
            completed += 1
            if completed == RESULT_LIMIT:
                break
    # Context exit has awaited producer and worker cleanup.
    return completed


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("Results: %s consumed", asyncio.run(main()))
```

Results arrive in completion order, which can differ from input order.
Leaving `async with` awaits producer and worker cleanup, including after `break`
or an exception. Sources and handlers must cooperate with cancellation.
Some additional items may already have started or finished before the break;
stopping does not undo their side effects.

Both helpers accept synchronous and asynchronous inputs. Each queue defaults to
`workers_count` items. Queue limits bound item counts, not payload bytes or
results retained by your code. `process_all()` still retains the full result list.

### 4. Fetch URLs with rate limits and retries

Install the optional HTTP client, then run this standalone example. It makes
real GET requests; replace `urls` with your endpoints.

```bash
python -m pip install aqute==0.10.0 httpx
```

Keep one client open around the managed result stream so workers finish cleanup
before their connections close.

```python
"""Fetch URLs with shared connections, rate limits, and transport retries."""

import asyncio
import logging

import httpx

from aqute import Aqute
from aqute.ratelimiter import TokenBucketRateLimiter


async def main() -> None:
    urls = ["https://example.com/", "https://example.org/"]
    async with httpx.AsyncClient(timeout=5.0) as client:

        async def fetch(url: str) -> int:
            response = await client.get(url)
            response.raise_for_status()
            return response.status_code

        engine = Aqute(
            fetch,
            workers_count=4,
            rate_limiter=TokenBucketRateLimiter(max_rate=5),
            retry_count=2,
            retry_delay=lambda _attempt, _error: 0.5,
            specific_errors_to_retry=httpx.TransportError,
        )
        async with engine.iter_results(urls) as results:
            async for task in results:
                if task.error is not None:
                    logging.warning("Failed %s: %s", task.data, task.error)
                else:
                    logging.info("%s: HTTP %s", task.data, task.unwrap())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
```

`workers_count=4` limits concurrent handlers. The limiter spaces attempt
starts by at least 0.2 seconds, including retries. `retry_count=2` allows at most
three attempts per URL, with a fixed 0.5-second delay before each retry.
HTTPX applies its five-second timeout to network operations, not to the entire
batch or retry sequence.

Only `httpx.TransportError` failures are retried. HTTP status errors, including
429 and 503, are logged as terminal failures and processing continues. Choose
retryable errors and operations for your service; retries can repeat side effects.
HTTPX reads each response body into memory even though this example returns only
the status code.

For HTTP 429 and `Retry-After` handling, see the [shared-pause HTTP recipe](https://insomnes.github.io/aqute/http_client/).
See [usage](https://insomnes.github.io/aqute/usage/) for retry filters, timeout contracts, queue overrides,
and manual processing. Manual runs must consume results concurrently with the
default finite queues.

Both helpers accept `submission_batch_size` (default `1`). Larger batches can
improve throughput for small tasks at the cost of result latency. Handlers still
receive one item per call; configure worker concurrency and queue limits separately.

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
