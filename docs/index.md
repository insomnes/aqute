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

## Quickstart

Install the library with `pip install aqute`. This complete example returns an
ordered list of doubled values and propagates handler errors.

```python
--8<-- "examples/quickstart.py"
```

From a development checkout, run `uv run --locked python -m examples.quickstart`.

`await engine.process_all(items)` returns a list in input order. For
completion-order streaming, use `async with engine.iter_results(items) as results`
and iterate `results` inside the context. Both helpers accept `Iterable` and
`AsyncIterable` inputs. Each terminal `AquteTask` exposes `data`, `task_id`, `result`,
`error`, and `success`. Inspect `error` or `success`: `None` can be a valid handler
result.

See [usage](usage.md) for buffering, cleanup, retry, timeout, and compatibility
contracts. The executable examples cover [streaming](streaming.md),
[bounded HTTP processing](http_client.md), [manual result draining](manual_drain.md),
and [service shutdown](service_shutdown.md).

## Bounded HTTP processing

Start with the [HTTP example](http_client.md) to combine four workers, a shared
HTTPX client, attempt-rate limits, selected retries, and incremental results.
The page includes the runnable source and explains its fail-fast error policy.

```bash
uv run --locked python -m examples.http_client
```

This checkout command runs offline. It completes a transport retry, logs two
pages, and handles a terminal HTTP error in a separate run. Each run uses a fresh
engine and a managed stream with finite queue defaults. It returns a count instead
of retaining every body. Queues bound items, not payload bytes or application data.
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
