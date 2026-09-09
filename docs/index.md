# Aqute

Aqute is an asyncio worker pool with configurable retries, rate limits, and
streaming results for independent I/O-bound jobs. It requires Python 3.11 or newer
and has no runtime dependencies.

Aqute runs in one process; pending work and results do not survive process
termination. CPU-heavy or blocking handlers block the event loop; Aqute does not
offload them to threads or processes.

Typical uses are API ingestion and backfills, infrastructure automation, and
independent remote inference or evaluation requests. Your application owns retry
safety, checkpoints, token budgets, and provider policy.

The [LLM batch inference example](llm_inference.md) shows application-owned
token-cost admission, shared throttling pauses, and checkpoints without a vendor SDK.

## Installation

These pages and examples cover the 0.10.0 API. With Python 3.11+, install Aqute:

```bash
python -m pip install aqute==0.10.0
```

## Quickstart

After [installing Aqute](#installation), save any example below as `quickstart.py`
and run `python quickstart.py`. The first three need only Aqute and the standard
library. The HTTP example also needs `httpx`.

The first three handlers use `await asyncio.sleep(uniform(0.025, 0.1))` to
simulate I/O with a random delay of 25–100 ms without blocking the event loop.
The HTTP handler awaits real network I/O.

### 1. Collect a finite batch in input order

Start here when the full result list fits in memory. The handler receives
one input item per call; `workers_count=4` permits up to four concurrent handlers.

<!-- example: examples/quickstart.py -->
```python
"""Run a finite batch and receive results in input order."""

import asyncio
import logging
from random import uniform

from aqute import Aqute


async def handle(value: int) -> int:
    await asyncio.sleep(uniform(0.025, 0.1))  # Simulate asynchronous I/O.
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
<!-- /example -->

The result is `[0, 2, 4, 6, 8, 10, 12, 14, 16, 18]`. `process_all()` returns
completed task objects in input order. `task.unwrap()` returns the handler value
or raises its error. Here, unwrapping happens after the whole batch finishes;
it does not stop the batch on its first failure.

### 2. Record errors and continue

Inspect `task.error` when one bad item must not prevent you from using the
other results. This example uses local conversion to make a failure reproducible.

<!-- example: examples/quickstart_errors.py -->
```python
"""Report each failed item and keep the successful results."""

import asyncio
import logging
from random import uniform

from aqute import Aqute


async def parse_port(value: str) -> int:
    await asyncio.sleep(uniform(0.025, 0.1))  # Simulate asynchronous I/O.
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
<!-- /example -->

This logs ports `443` and `8080`, plus a failure for `"invalid"`. Handler
failures are stored on completed tasks; retries are disabled by default.
Check `error` or `success`, not whether `result` is `None`: a successful handler
can return `None`. Input-source and engine errors can still raise from a helper.

### 3. Consume a stream and stop early

Use `iter_results()` to consume results as they complete without collecting
the full output. This example takes an asynchronous input source and stops
after three results.

<!-- example: examples/streaming.py -->
```python
"""Consume bounded streaming results and stop early with managed cleanup."""

import asyncio
import logging
from random import uniform

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
        await asyncio.sleep(uniform(0.025, 0.1))  # Simulate asynchronous I/O.
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
<!-- /example -->

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

<!-- example: examples/quickstart_http.py -->
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
<!-- /example -->

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

For HTTP 429 and `Retry-After` handling, see the [shared-pause HTTP recipe](http_client.md).
See [usage](usage.md) for retry filters, timeout contracts, queue overrides,
and manual processing. Manual runs must consume results concurrently with the
default finite queues.

Both helpers accept `submission_batch_size` (default `1`). Larger batches can
improve throughput for small tasks at the cost of result latency. Handlers still
receive one item per call; configure worker concurrency and queue limits separately.

From a development checkout, run the dependency-free examples directly:

```bash
uv run --locked python -m examples.quickstart
uv run --locked python -m examples.quickstart_errors
uv run --locked python -m examples.streaming
```

`uv run --locked python -m examples.quickstart_http` makes real network requests.
The HTTP recipe below provides a separate offline example.

## Bounded HTTP processing

Start with the [HTTP example](http_client.md) to combine four workers, a shared
HTTPX client, attempt-rate limits, selected retries, and incremental results.
The page includes the runnable source and explains its fail-fast error policy.

```bash
uv run --locked python -m examples.http_client
```

This checkout command runs offline. It first compares throttling with and without
a shared pause. It then completes a transport retry, logs two pages, and handles a
terminal HTTP error in a separate run. Each run uses a fresh engine and a managed
stream with finite queue defaults. `fetch_pages()` returns a count instead of
retaining every body. Queues bound items, not payload bytes or application data.
The same source provides an explicit callable path for real requests.

For independent callers awaiting their own replies, see the
[per-caller request pool](request_pool.md).
See [For coding agents](usage.md#for-coding-agents) for helper selection and
application ownership rules.

## When to choose Aqute

| Option | Documented behavior | Choose it when |
| --- | --- | --- |
| Plain asyncio | [`gather()`](https://docs.python.org/3/library/asyncio-task.html#asyncio.gather) collects results in input order. [`TaskGroup`](https://docs.python.org/3/library/asyncio-task.html#task-groups) awaits its tasks on context exit. | A small finite batch only needs concurrent calls, or your application already owns retries and flow control. |
| [aiometer](https://github.com/florimondmanca/aiometer#usage) | `max_at_once` limits concurrent tasks; `max_per_second` limits starts per second. `run_all()` collects ordered results; `amap()` streams results as they become available. It supports asyncio and Trio. | You need concurrency and start-rate limits with result collection, and prefer to keep retry policy in your handler. |
| Aqute | The [worker-pool API](usage.md) combines retry filters and delays, per-task success or error outcomes, synchronous or asynchronous input, and explicit shutdown. | Repeated I/O jobs need these controls together, such as an ingestion run that retries selected failures and records each terminal outcome. |
| Broker-backed queues ([arq](https://arq-docs.helpmanual.io/), [Taskiq](https://taskiq-python.github.io/), [Dramatiq](https://dramatiq.io/), [Celery](https://docs.celeryq.dev/)) | Workers exchange tasks through a configured broker; deployment and delivery guarantees depend on the chosen system. Aqute runs in-process and needs no broker infrastructure. | Work must outlive the submitting process or run across independently deployed workers, and you can operate the broker and workers. |

For infrastructure changes, retries can repeat side effects; the application must
decide which operations are safe to repeat. For remote inference, Aqute schedules
handler attempts; model execution and provider-specific policy remain outside it.

`process_all()`, `iter_results()`, and manual processing use finite input and result
buffering by default, with each queue sized to `workers_count`. Manual runs must
consume results concurrently or [choose unlimited queues explicitly](usage.md#queue-default-migration).
Queue limits bound items, not bytes; `process_all()` retains the complete result
list. See [buffering](usage.md#buffering) for overrides, the item bound, and migration.

The `iter_results()` context awaits producer and worker cleanup, including after
early exit. See [streaming and cleanup](usage.md#streaming-and-cleanup) for the
lifecycle contract.
