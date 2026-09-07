# Usage

## Buffering and iterator cleanup

Set both `input_task_queue_size=I` and `result_queue=asyncio.Queue(R)` to positive
values to bound buffering. With `W` workers and one producer, Aqute retains at most
`I + 2*R + W + 3` input/result items, including pending admission and the current
yielded item. The internal worker-result queue uses capacity `R` too. Slow result
consumption blocks further processing and admission.

This is an item-count bound, not a byte limit. It excludes items retained by the
source or caller and the growing list returned by `process_all()`. A queue size of
zero is unlimited. Multiple independent producer calls can each hold an item
while waiting for admission; the formula above assumes one producer.

Use `async with contextlib.aclosing(engine.iter_results(items)) as results` when
iteration can stop early. `async for` does not close an async generator after
`break`. Closing or cancelling the iterator waits for its producer and workers.
Aqute closes generator sources; callers own other source resources. Cleanup
requires handlers and rate limiters to cooperate with cancellation.

Before reusing an engine with a helper, call `drain_results()` to remove retained
results. Otherwise, a helper can consume results from the previous run. Use a
fresh engine when those results must remain in their original queue. Helpers own
the run; use the manual API for externally managed producers and consumers.

## Concurrency, rate, and priority

`workers_count` limits occupied workers. It does not specify requests per second.
Use `rate_limiter` to control attempt rate. Every retry acquires the limiter again.
Available implementations are in `aqute.ratelimiter`:

- `TokenBucketRateLimiter` controls a shared rate, with optional bursts.
- `SlidingRateLimiter` limits calls within a moving time window.
- `PerWorkerRateLimiter` applies a separate token bucket to each worker.
- `RandomizedIntervalRateLimiter` randomizes intervals around the configured rate.

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
| `apply_to_each(items)` | `iter_results(items)` |

`start()`, `stop()`, and `add_task()` keep their names. Generic handler input and
result types are preserved through both interfaces and the async context manager.
For example, a handler accepting `int` makes `add_task("text")` a type error;
this is an intentionally invalid call, not a runnable usage example.
