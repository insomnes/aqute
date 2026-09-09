# Bounded HTTP processing

Process independent GET requests with one shared HTTPX client and a fresh Aqute
engine per run. The example combines four workers, an attempt-rate limiter,
selected retries, and incremental result consumption.

This recipe uses the 0.10.1 API for Python 3.11+. Follow the
[installation instructions](index.md#installation).

## Use in an application

For real requests, call `await fetch_pages(urls)` without a transport. Install
[Aqute](index.md#installation) and `httpx` in your
application environment. HTTPX is a development dependency in this checkout;
it is not an Aqute runtime dependency.

The source imports `retry_delay` from the checkout's `examples.retry_progress`,
which is not installed with `aqute`. To adapt it outside the checkout, copy the
callback and its `from random import uniform` import from the
[retry recipe](retry_progress.md), or provide your own callback. Replace the
`examples.retry_progress` import with your application's import, or remove it if
you define the callback in the same file.

## Limits and outcomes

In `fetch_pages()`, `workers_count=4` limits occupied workers. The inner
`TokenBucketRateLimiter(max_rate=10)` spaces its grants by at least 0.1 seconds,
including retries, with one initial grant available immediately.
`PausableRateLimiter` adds a shared pause before and after the inner grant.
Grants delayed by a pause can resume together; the wrapper does not reacquire
permits or guarantee request spacing after that delay. Each client request uses
HTTPX's five-second timeout setting.

`retry_count=2` permits at most three attempts per URL. The recipe selects
`httpx.TransportError` and HTTP 429, represented by `RateLimitedError`, for retries.
Other HTTP status errors, including 503, are terminal. Transport errors use the
[retry callback](retry_progress.md) for capped exponential backoff with jitter.

On HTTP 429, `fetch_pages()` applies a shared pause from `Retry-After`. The retry
callback also uses that header for the failed worker's retry delay. The parser
accepts nonnegative numeric seconds or HTTP dates. Missing, malformed, negative,
or nonfinite values use one second; past dates use zero. Valid delays have no
upper bound. Apply any required cap in application code. See the
[pause limits](reference.md#shared-pauses) for admission and timeout
behavior. Applications must select retryable errors and operations for their
own service.

Aqute yields handler failures as terminal task error values. This example calls
`task.unwrap()` to raise the first observed error and stop the run. Already logged
pages remain logged; the remaining URLs might not complete. The offline entry
point catches its expected HTTP error explicitly. Inspect `task.error` instead
when your application must record failures and continue consuming results.

The managed `iter_results()` context uses finite queues by default. With four
workers, each input and result queue has capacity four. To choose other positive
capacities, set `input_task_queue_size=I` and `result_queue=asyncio.Queue(R)` on
the constructor. Zero means unlimited. See [buffering](usage.md#buffering) for
the complete item bound and override rules.

`fetch_pages()` logs each successful body as it arrives and returns only a count.
It does not build a list of bodies. HTTPX still reads each complete response into
memory. Queue capacities bound items, not bytes; source data, payload sizes, and
data retained by application logging or persistence remain outside that bound.

## Resource and retry ownership

The stream context stops the producer and workers before the shared client closes,
including when `unwrap()` or the input source raises. `fetch_pages()` owns its
client and any supplied transport for that call. Aqute closes started generator
sources; callers must explicitly close other source resources. See
[streaming cleanup](usage.md#streaming-and-cleanup) for cancellation and early exit.

The inputs are URL strings that can be reused for another attempt. To repeat a
whole run, recreate an exhausted generator or use a replayable URL collection.
Your application owns retry safety, duplicate side effects, and durable progress.
Replace the log statement with application-owned parsing or persistence as needed;
a logged response does not establish a durable checkpoint.

See [Choose an API](index.md#choose-an-api) for a compact application-usage
guide, and [manual processing](usage.md#manual-processing-and-shutdown) for
externally managed producers and consumers.

## Run the offline demonstration

From a development checkout, run the example offline:

```bash
uv run --locked python -m examples.http_client
```

The HTTP transport is the only fake. The entry point first compares eight workers
processing forty URLs with and without a shared pause during a 300 ms throttle
window. Both variants use `Retry-After: 1` and a 100-attempt/second inner limiter.
The transport responds immediately, before the next admission. It logs throttled
request counts, retries, and elapsed time. Requests already in flight can still
return 429 after a pause; this controlled comparison does not bound their count
or predict real-service throughput.

Next, it fetches two pages after one temporary connection failure. A separate run
returns HTTP 503, which the example reports as a terminal error. The entry point
logs both page bodies and ends with `Results: 2 pages`. Each run uses a separate
client and engine.

## Runnable source

<!-- example: examples/http_client.py -->
```python
"""Use one shared HTTPX client; main() runs an offline transport demonstration."""

import asyncio
import logging
import math
from collections.abc import AsyncIterable, Iterable
from email.utils import parsedate_to_datetime
from functools import partial
from time import monotonic, time

import httpx

from aqute import Aqute
from aqute.ratelimiter import PausableRateLimiter, TokenBucketRateLimiter
from examples.retry_progress import retry_delay

logger = logging.getLogger(__name__)
TOO_MANY_REQUESTS = 429


class RateLimitedError(httpx.HTTPStatusError):
    """Select HTTP 429 for retries without retrying other HTTP status errors."""


def retry_after_seconds(value: str | None) -> float:
    """Accept nonnegative numeric seconds or an HTTP-date; default to one second.

    Missing, malformed, negative, or nonfinite values use the fallback. Past
    HTTP dates return zero. HTTP-date conversion uses wall time only here;
    PausableRateLimiter stores the resulting delay on the monotonic clock.
    """
    if value is None:
        return 1.0
    try:
        seconds = float(value)
    except ValueError:
        try:
            deadline = parsedate_to_datetime(value)
            if deadline.tzinfo is None:
                return 1.0
            return max(0.0, deadline.timestamp() - time())
        except (TypeError, ValueError, OverflowError):
            return 1.0
    return seconds if math.isfinite(seconds) and seconds >= 0 else 1.0


def http_retry_delay(failed_attempt: int, error: Exception) -> float:
    """Keep per-worker retry delay for the comparison and transport backoff."""
    if isinstance(error, RateLimitedError):
        return retry_after_seconds(error.response.headers.get("Retry-After"))
    return retry_delay(failed_attempt, error)


async def fetch_page(
    client: httpx.AsyncClient, url: str, limiter: PausableRateLimiter | None
) -> str:
    """Pause shared admission on 429; None demonstrates only per-worker delay."""
    response = await client.get(url)
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as error:
        if response.status_code != TOO_MANY_REQUESTS:
            raise
        if limiter is not None:
            limiter.pause_for(retry_after_seconds(response.headers.get("Retry-After")))
        raise RateLimitedError(
            str(error), request=error.request, response=error.response
        ) from error
    return response.text


async def fetch_pages(
    urls: Iterable[str] | AsyncIterable[str],
    *,
    transport: httpx.AsyncBaseTransport | None = None,
) -> int:
    """Log pages as they complete; raise the first observed terminal error."""
    limiter = PausableRateLimiter(TokenBucketRateLimiter(max_rate=10))
    async with httpx.AsyncClient(timeout=5.0, transport=transport) as client:

        async def fetch(url: str) -> str:
            return await fetch_page(client, url, limiter)

        engine = Aqute(
            fetch,
            workers_count=4,
            rate_limiter=limiter,
            retry_count=2,
            retry_delay=http_retry_delay,
            specific_errors_to_retry=(httpx.TransportError, RateLimitedError),
        )
        completed = 0
        async with engine.iter_results(urls) as results:
            async for task in results:
                # Replace this log with application-owned parsing or persistence.
                logger.info("Fetched %s: %s", task.data, task.unwrap())
                completed += 1
        return completed


async def compare_throttling() -> None:
    """Compare eight workers against one 300 ms throttle window, offline.

    The transport responds immediately, before the next paced admission. Real
    requests already in flight can produce more 429s after a pause is set.
    These request counts do not predict throughput for a real service.
    """
    workers = 8
    urls = [f"https://example.test/jobs/{value}" for value in range(40)]
    for shared_pause in (False, True):
        throttled = 0
        window_end = 0.0

        def respond(request: httpx.Request) -> httpx.Response:
            nonlocal throttled, window_end
            if window_end == 0:
                window_end = monotonic() + 0.3
            if monotonic() < window_end:
                throttled += 1
                return httpx.Response(429, headers={"Retry-After": "1"})
            return httpx.Response(200, text=request.url.path)

        inner = TokenBucketRateLimiter(max_rate=100)
        limiter = PausableRateLimiter(inner) if shared_pause else None
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            engine = Aqute(
                partial(fetch_page, client, limiter=limiter),
                workers_count=workers,
                rate_limiter=limiter if limiter is not None else inner,
                retry_count=2,
                retry_delay=http_retry_delay,
                specific_errors_to_retry=(httpx.TransportError, RateLimitedError),
            )
            started = monotonic()
            completed = retries = 0
            async with engine.iter_results(urls) as results:
                async for task in results:
                    task.unwrap()
                    completed += 1
                    retries = engine.counters.retries
            logger.info(
                "Throttle comparison: shared_pause=%s workers=%s pages=%s "
                "throttled_requests=%s retries=%s elapsed=%.3fs",
                shared_pause,
                workers,
                completed,
                throttled,
                retries,
                monotonic() - started,
            )


async def main() -> int:
    failed_once = False

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal failed_once
        if request.url.path == "/jobs/1" and not failed_once:
            failed_once = True
            raise httpx.ConnectError("temporary connection failure", request=request)
        if request.url.path == "/unavailable":
            return httpx.Response(503)
        return httpx.Response(200, text=request.url.path)

    completed = await fetch_pages(
        ("https://example.test/jobs/1", "https://example.test/jobs/2"),
        transport=httpx.MockTransport(respond),
    )
    try:
        await fetch_pages(
            ("https://example.test/unavailable",),
            transport=httpx.MockTransport(respond),
        )
    except httpx.HTTPStatusError as error:
        logger.error(
            "Terminal HTTP %s for %s; run stopped",
            error.response.status_code,
            error.request.url,
        )
    return completed


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(compare_throttling())
    logging.info("Results: %s pages", asyncio.run(main()))
```
<!-- /example -->
