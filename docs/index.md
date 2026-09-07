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

`process_all(items)` returns a list in input order. `iter_results(items)` yields
terminal results in completion order. Both accept `Iterable` and `AsyncIterable`
inputs. Each `AquteTask` exposes `data`, `task_id`, `result`, `error`, and `success`.
Inspect `error` or `success`: `None` can be a valid handler result.

See [usage](usage.md) for buffering, cleanup, retry, timeout, and compatibility
contracts. The executable examples cover [streaming](streaming.md), a shared
[HTTP client](http_client.md), and [service shutdown](service_shutdown.md).

## When to choose Aqute

| Option | Documented behavior | Choose it when |
| --- | --- | --- |
| Plain asyncio | [`gather()`](https://docs.python.org/3/library/asyncio-task.html#asyncio.gather) collects results in input order. [`TaskGroup`](https://docs.python.org/3/library/asyncio-task.html#task-groups) awaits its tasks on context exit. | A small finite batch only needs concurrent calls, or your application already owns retries and flow control. |
| [aiometer](https://github.com/florimondmanca/aiometer#usage) | `max_at_once` limits concurrent tasks; `max_per_second` limits starts per second. `run_all()` collects ordered results; `amap()` streams results as they become available. It supports asyncio and Trio. | You need concurrency and start-rate limits with result collection, and prefer to keep retry policy in your handler. |
| Aqute | The [worker-pool API](usage.md) combines retry filters and delays, per-task success or error outcomes, synchronous or asynchronous input, and explicit shutdown. | Repeated I/O jobs need these controls together, such as an ingestion run that retries selected failures and records each terminal outcome. |

For infrastructure changes, retries can repeat side effects; the application must
decide which operations are safe to repeat. For remote inference, Aqute schedules
handler attempts; model execution and provider-specific policy remain outside it.

Configure both input and result queue limits to bound buffering; their defaults
are unlimited. `process_all()` retains the complete result list. If streaming can
stop early, close `iter_results()` with `contextlib.aclosing`. See
[buffering and iterator cleanup](usage.md#buffering-and-iterator-cleanup) for the
current lifecycle contract.
