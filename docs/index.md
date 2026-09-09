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

## Installation

These pages and examples cover the 0.10.0 API. With Python 3.11+, install Aqute:

```bash
python -m pip install aqute==0.10.0
```

## Quickstart

After [installing Aqute](#installation), save any example below as `quickstart.py`
and run `python quickstart.py`. The first three need only Aqute and the standard
library. The HTTP example also needs `httpx`.

The first three handlers use `await asyncio.sleep(0.1)` to simulate I/O
without blocking the event loop. The HTTP handler awaits real network I/O.

### 1. Collect a finite batch in input order

Start here when the full result list fits in memory. The handler receives
one input item per call; `workers_count=4` permits up to four concurrent handlers.

```python
--8<-- "examples/quickstart.py"
```

The result is `[0, 2, 4, 6, 8, 10, 12, 14, 16, 18]`. `process_all()` returns
completed task objects in input order. `task.unwrap()` returns the handler value
or raises its error. Here, unwrapping happens after the whole batch finishes;
it does not stop the batch on its first failure.

### 2. Record errors and continue

Inspect `task.error` when one bad item must not prevent you from using the
other results. This example uses local conversion to make a failure reproducible.

```python
--8<-- "examples/quickstart_errors.py"
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
--8<-- "examples/streaming.py"
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
--8<-- "examples/quickstart_http.py"
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

See [For coding agents](usage.md#for-coding-agents) for helper selection and
application ownership rules.

## When to choose Aqute

| Option | Documented behavior | Choose it when |
| --- | --- | --- |
| Plain asyncio | [`gather()`](https://docs.python.org/3/library/asyncio-task.html#asyncio.gather) collects results in input order. [`TaskGroup`](https://docs.python.org/3/library/asyncio-task.html#task-groups) awaits its tasks on context exit. | A small finite batch only needs concurrent calls, or your application already owns retries and flow control. |
| [aiometer](https://github.com/florimondmanca/aiometer#usage) | `max_at_once` limits concurrent tasks; `max_per_second` limits starts per second. `run_all()` collects ordered results; `amap()` streams results as they become available. It supports asyncio and Trio. | You need concurrency and start-rate limits with result collection, and prefer to keep retry policy in your handler. |
| Aqute | The [worker-pool API](usage.md) combines retry filters and delays, per-task success or error outcomes, synchronous or asynchronous input, and explicit shutdown. | Repeated I/O jobs need these controls together, such as an ingestion run that retries selected failures and records each terminal outcome. |

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
