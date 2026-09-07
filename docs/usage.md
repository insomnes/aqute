# Usage

## Task results

Call `AquteTask.unwrap()` on terminal results obtained from `Aqute` through
`process_all()`, `iter_results()`, `get_result()`, or `drain_results()`. It returns
`TResult` on success, including `None` when that is a valid handler result.
On failure, it raises the stored exception. A pending task raises `AquteError`.

`unwrap()` does not change the task's outcome fields. Keep inspecting `data`,
`result`, `error`, and `success` when collecting partial failures.

The [runnable quickstart](index.md#quickstart) collects values with
`values = [task.unwrap() for task in tasks]` after `process_all()` returns.
The batch has already finished when `unwrap()` runs, so this does not make
processing fail fast.

## Streaming and cleanup

Use an async context to own streaming work:

```python
async with engine.iter_results(items) as results:
    async for task in results:
        consume(task)
```

Each call returns a context for one lazy, single-use iterator. The first iteration
starts processing. Entering and leaving the context without iteration does not
consume input or acquire ownership of its source. Results arrive in completion
order as terminal `AquteTask` objects. `await engine.process_all(items)` owns the
same context internally and returns a list in input order.

Context exit waits for the producer and workers to stop after full consumption,
an early `break`, a consumer exception, a source exception, or cancellation.
Source and consumer errors propagate; handler errors remain task error values.
Caller cancellation, including a deadline during cleanup, propagates through the
existing cooperative cleanup path. Cleanup requires sources, handlers, and rate
limiters to cooperate with cancellation; it cannot forcibly terminate them.
Aqute closes started generator sources. Callers own other source resources,
including custom iterators with `close()` or `aclose()` methods.

Use each context and iterator once, and consume results inside the context.
Use the engine sequentially. Before reusing it with a helper after partial
consumption, call `drain_results()` to remove retained results. Helpers reject
retained results in an automatically created result queue. Drain supplied queues
too; helpers consume from them. Use a fresh engine when those results must remain
in their original queue. Helpers require a fresh or stopped engine with no pending
manual tasks. Conflicting setup raises `AquteError` before consuming input. Use the
manual API for externally managed producers and consumers.

## Buffering

`process_all()` and `iter_results()` use finite buffering by default. With `W`
workers, omitted limits give each queue capacity `W`: pending input, terminal
results, and the internal worker-result queue. Slow result consumption blocks
further processing and admission.

| Constructor option | Helper runs | Manual runs |
| --- | --- | --- |
| Omitted `input_task_queue_size`, or `None` | Capacity `W` | Unlimited |
| `input_task_queue_size=I`, with `I > 0` | Capacity `I` | Capacity `I` |
| `input_task_queue_size=0` | Unlimited | Unlimited |
| Omitted `result_queue`, or `None` | Capacity `W` per result queue | Unlimited |
| `result_queue=asyncio.Queue(R)` | Supplied queue and relay use `R` | Supplied queue and relay use `R` |

A supplied result queue keeps its identity and capacity, including explicit
`asyncio.Queue(0)` for unlimited results. Aqute does not resize or replace it.
After helper cleanup, manual runs keep their original queue limits and already
published results remain available through `get_result()` or `drain_results()`.

With positive input capacity `I`, result capacity `R`, and one producer, Aqute
retains at most `I + 2*R + W + 3` input/result items, including pending admission,
the collector-held result, and the currently yielded item. The default helper
bound is `4*W + 3` items. The internal worker-result queue uses capacity `R` too.

This is an item-count bound, not a byte limit. It excludes items retained by the
source or caller and the growing list returned by `process_all()`. A queue size of
zero is unlimited. Multiple independent producer calls can each hold an item
while waiting for admission; the formula above assumes one producer.

This pre-1.0 change replaces the constructor default
`input_task_queue_size: int = 0` with `input_task_queue_size: int | None = None`.
Existing positive capacities still override the defaults. To preserve the old
unlimited helper behavior, set both limits explicitly:

```python
engine = Aqute(
    handle,
    workers_count=32,
    input_task_queue_size=0,
    result_queue=asyncio.Queue(0),
)
```

Manual pre-submit-then-run and run-then-drain flows with omitted limits need no
migration. Deprecated helper names inherit the new helper defaults.

## Concurrency, rate, and priority

`workers_count` limits occupied workers. It does not specify requests per second.
Use `rate_limiter` to control attempt rate. Every retry acquires the limiter again.
Available implementations are in `aqute.ratelimiter`:

- `TokenBucketRateLimiter` controls a shared rate, with optional bursts.
- `SlidingRateLimiter` limits calls within a moving time window.
- `PerWorkerRateLimiter` applies a separate token bucket to each worker.
- `RandomizedIntervalRateLimiter` adds bounded random delays after rolling-cap waits.

`RandomizedIntervalRateLimiter(N, T)` grants at most `N` acquisitions in each
rolling `T` seconds. Each acquisition waits for quota, then for its full additional
random delay. This delay also applies at startup, with sparse traffic and after
idle. Quota waits and event-loop scheduling can increase the total wait beyond the
configured jitter bounds. Sustained throughput can be below `N / T`.

`mean_target_multiplier` and `std_dev` describe the Gaussian input, before scaling
and bounding. They do not specify the mean or deviation of emitted intervals.
An independent uniform phase supplies an absolute sine scale for each acquisition.
The same scale multiplies the Gaussian input and `lower_upper_fluctuation`, which
moves both multiplier bounds inward. The resulting bounded multiplier converts
to seconds through `T / N`. Use nonnegative bounds and fluctuation, with
`2 * lower_upper_fluctuation <= upper_multiplier_bound - lower_multiplier_bound`.
The lower bound remains an additional minimum delay even when quota is available.

Independent phases replace the previous request-counter ordering while retaining
coupled amplitude and bound modulation. Seeded sequences and startup delays change.
This reduces conspicuous counter-linked regularity; it does not model human
behavior or guarantee avoidance of detection. Removing the old ordering can also
reduce throughput, even when the overall delay distribution is similar.

A custom limiter implements `async acquire(name="", task=None)`. It must propagate
cancellation. CPU-heavy or blocking work in a handler blocks the event loop;
Aqute does not move it to threads or processes automatically.

With `use_priority_queue=True`, pass `task_priority` to `add_task()`. Lower values
run first among admitted pending tasks. Priority does not preempt an occupied
worker. A worker retains its task through retries.

## Errors, retries, and timeouts

Handler exceptions become `AquteTask.error` values. `retry_count` specifies extra
attempts; its default is zero. `specific_errors_to_retry` selects exception types.
`errors_to_not_retry` excludes types and takes precedence when both filters match.

`retry_delay(failed_attempt, error)` optionally returns finite, nonnegative seconds.
The failed-attempt number is 1-based. The callback runs only when another attempt
is permitted. The default delay is zero. The [streaming example](streaming.md)
contains a capped exponential-backoff callback with jitter.

A delay occupies its worker but is outside the handler timeout. Other workers can
continue. Cancellation interrupts the delay. Invalid delays raise `ValueError`
inside the worker `ExceptionGroup`. Callback errors and limiter failures also stop
the worker group and propagate; they are not handler error values. A retry can
repeat a side effect, so applications must decide whether retrying is safe.

`task_timeout_seconds` limits each handler attempt and produces
`AquteTaskTimeoutError`. Rate-limit and retry-delay waits are excluded. A zero or
negative timeout prevents handler invocation. Add `AquteTaskTimeoutError` to
`errors_to_not_retry` when timeouts must not be retried.

`start_timeout_seconds` limits waiting for the first input after start, including
an asynchronous source's first item. `total_failed_tasks_limit` stops processing
with `AquteTooManyTasksFailedError` when collected terminal failures reach the limit.
Source exceptions propagate after helper cleanup.

The [HTTP example](http_client.md) uses one shared HTTPX client and retries
transport errors. HTTP status failures propagate to its caller. Its executable
entry point uses an offline transport; applications call `fetch_pages(urls)` for
real requests. HTTPX is required only for this example and is included in the dev
group. See [HTTPX's async client guide](https://www.python-httpx.org/async/).

## Manual processing and shutdown

Use the Aqute async context manager, or `start()` followed by `stop()`. Start workers
before filling a bounded input queue. Submit with `await add_task(data)` and
consume with `await get_result()`. Custom IDs use `task_id`; omitted IDs are
generated. `drain_results()` removes currently available results without waiting.
`get_result()` raises `AquteError` after normal completion when no results remain.

`finish_submitting()` signals that input submission is complete. `await finish()`
also sends that signal and waits for processing. Stop producers before either
call. These methods do not reject later submissions while the run still has work.
`await run()` starts processing and finishes work submitted before start.
After `run()` or `finish()` completes, await `stop()` before starting a helper.

The executable [service shutdown example](service_shutdown.md) shows:

1. Signal the producer to stop accepting new input.
2. Keep the result consumer running while the producer completes its current submission.
3. Await `finish()` and result draining under `asyncio.timeout`.
4. On deadline expiry, cancel the producer and consumer, then await `stop()`.

`stop()` cancels processing and waits for worker cleanup. It does not drain pending
input. A drain deadline requests cancellation; it cannot force an uncooperative
coroutine to terminate. The example's caller receives whether draining timed out.
In-process completion does not imply durable delivery.

Completed results remain available after `stop()`. The engine can be reused; drain
retained results before using a helper again. For lower-level worker queues,
`aqute.worker.Foreman` exposes `start()`, `add_task(AquteTask(...))`,
`get_handled_task()`, `finalize()`, and `stop()`. Consume finite result queues while
waiting for `finalize()`.

Inspect `error` and `result` on tasks returned directly by `Foreman`. It does not
set `success`; `unwrap()` requires the engine's terminal success flag to return
a value.

## Progress counters

`engine.counters` returns an immutable `AquteCounters` snapshot for the current run.

| Counter | Meaning |
| --- | --- |
| `pending` | Admitted tasks awaiting a worker; excludes blocked submissions. |
| `running` | Occupied workers, including rate-limit, retry-delay, and result-publication waits. |
| `succeeded` | Terminal successful handler outcomes. |
| `failed` | Terminal failed handler outcomes after retry rules; excludes cancelled work. |
| `retries` | Additional handler invocations that started; excludes retries still waiting. |

Reading or draining results does not change terminal counts. A worker can have a
terminal outcome while waiting to publish it, so `running` can overlap with the
terminal counts. `stop()` resets all counts after cleanup. Retained results do not
become counts in a later run.

Helpers stop on exit. Inspect their counters during iteration, or use the manual
flow to inspect them after `finish()` and before `stop()`. Both the streaming and
service examples log these snapshots.

## Public names and compatibility

Use the replacement names in new code. Old names remain compatible wrappers and
emit `DeprecationWarning` at the caller. Removal will occur only in an announced
breaking release; no removal version is currently scheduled.

| Deprecated name | Replacement |
| --- | --- |
| `set_all_tasks_added()` | `finish_submitting()` |
| `wait_till_end()` | `await finish()` |
| `start_and_wait()` | `await run()` |
| `get_task_result()` | `await get_result()` |
| `extract_all_results()` | `drain_results()` |
| `apply_to_all(items)` | `await process_all(items)` |
| `apply_to_each(items)` | `async with iter_results(items) as results` |

`start()`, `stop()`, and `add_task()` keep their names. Generic handler input and
result types are preserved through both interfaces and the async context manager.
For example, a handler accepting `int` makes `add_task("text")` a type error;
this is an intentionally invalid call, not a runnable usage example.

### Managed streaming migration

This is a pre-1.0 signature and typing break for `iter_results()`. It is now a
regular method returning
`AbstractAsyncContextManager[AsyncIterator[AquteTask[TData, TResult]]]`, rather
than an async generator returning `AsyncGenerator[AquteTask[TData, TResult]]`.
The input remains `Iterable[TData] | AsyncIterable[TData]`; the keyword-only
`submission_batch_size: int = 1` is unchanged. Validation still occurs on first
iteration, before input consumption. No extra import or exported type is needed
for normal use.

Before:

```python
from contextlib import aclosing

async with aclosing(engine.iter_results(items)) as results:
    async for task in results:
        consume(task)
```

After:

```python
async with engine.iter_results(items) as results:
    async for task in results:
        consume(task)
```

Also replace bare `async for task in engine.iter_results(items)` with the new
context form. Call `anext(results)` on the iterator yielded by the context.
Do not call `aclose()` on the context returned by `iter_results()`.

Deprecated `apply_to_each(items)` retains its `AsyncGenerator` return type and
warning. Existing `async for` callers still work. Use
`async with contextlib.aclosing(engine.apply_to_each(items)) as results` or
explicitly await that generator's `aclose()` when stopping early. Its compatibility
wrapper shares the same processing and cleanup path; migrate new code to the
managed `iter_results()` form. `process_all()` keeps its coroutine signature and
ordered list return type.

## For coding agents

For application code, install with `pip install aqute` and import
`Aqute` from `aqute`. The [canonical HTTP recipe](http_client.md#runnable-source)
also requires `httpx`; the checkout's dev group includes it. Adapt that runnable
source instead of reconstructing the API from older examples.

| Application need | API |
| --- | --- |
| A finite batch with an ordered result list | `await engine.process_all(items)` |
| Results as they complete, with incremental consumption | `async with engine.iter_results(items) as results`, then iterate `results` inside the context |
| Separate producer and consumer ownership | Follow [manual processing and shutdown](#manual-processing-and-shutdown) |

- Create a fresh engine for each helper run. The stream context waits for cleanup
  after completion, early exit, or failure. Keep the external client open around
  that context. Aqute closes started generator sources; callers own other source
  resources and must close them explicitly.
- Leave queue limits omitted for finite helper defaults, with each queue sized to
  `workers_count`. Use positive capacities to override them; zero is unlimited.
  These are item limits. Payload bytes, caller-retained data, and the complete
  list returned by `process_all()` are outside the bound. See [buffering](#buffering).
- Handle each terminal task explicitly. `task.unwrap()` returns the successful
  value, including a valid `None`, or raises its error. Inspect `task.error` or
  `task.success` when collecting partial failures. A false or `None` result does
  not prove failure. See [task results](#task-results).
- Select safe retry errors and a finite retry count. Every retry acquires the
  attempt-rate limiter again; worker count controls concurrency separately.
  Keep inputs replayable. Applications own repeated side effects, client policy,
  and durable progress. See [retry rules](#errors-retries-and-timeouts).
