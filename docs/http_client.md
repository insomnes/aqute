# Bounded HTTP processing

Process independent GET requests with one shared HTTPX client and a fresh Aqute
engine per run. The example combines four workers, an attempt-rate limiter,
selected retries, and incremental result consumption.

This recipe uses the development API for Python 3.11+. Follow the
[development installation instructions](index.md#development-installation).
For PyPI 0.9.3, use its versioned
[README](https://github.com/insomnes/aqute/blob/0.9.3/README.md#quickstart).

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

For real requests, call `await fetch_pages(urls)` without a transport. Install the
[development source](index.md#development-installation) and `httpx` in your
application environment. HTTPX is a development dependency in this checkout;
it is not an Aqute runtime dependency.

The source imports `retry_delay` from the checkout's `examples.retry_progress`,
which is not installed with `aqute`. To adapt it outside the checkout, copy the
callback and its `from random import uniform` import from the
[retry recipe](retry_progress.md), or provide your own callback. Replace the
`examples.retry_progress` import with your application's import, or remove it if
you define the callback in the same file.

## Runnable source

```python
--8<-- "examples/http_client.py"
```

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
[pause limits](usage.md#concurrency-rate-and-priority) for admission and timeout
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

See [For coding agents](usage.md#for-coding-agents) for a compact application-usage
guide, and [manual processing](usage.md#manual-processing-and-shutdown) for
externally managed producers and consumers.
