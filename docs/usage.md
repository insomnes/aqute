# Usage

<span id="for-coding-agents"></span>

This guide covers the 0.10.1 API for Python 3.11+. Start with
[installation](index.md#installation) and [Choose an API](index.md#choose-an-api).
See the [API reference](reference.md) for parameter types and defaults, or the
[changelog](https://github.com/insomnes/aqute/blob/main/CHANGELOG.md) for migration
from 0.9.x.

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
For one result at a time, call `await anext(results)` on the yielded iterator.
Use the engine sequentially. Before reusing it with a helper after partial
consumption, retrieve all retained results. Both helpers raise `AquteError` when
results remain in the result queue, including a caller-supplied queue. Rejection
occurs before acquiring the new source or invoking its handler. Retained outcomes
remain unchanged and available through `get_result()` or `drain_results()`.
Drain them explicitly before retrying:

```python
previous_results = engine.drain_results()
new_results = await engine.process_all(new_items)
```

The new helper run returns only its own results. Use a fresh engine when old
results must remain in their original queue. Helpers require a fresh or stopped
engine with no pending manual tasks. Conflicting setup raises `AquteError` before
consuming input. Preloading with `add_task()` before either helper is rejected.
Use the manual API to finish such work while draining results, then stop the
engine before using a helper. Use a fresh engine for each helper run when reuse
is unnecessary. Helpers accept synchronous and asynchronous inputs; recreate an
exhausted generator before repeating a run.

## Buffering

All Aqute runs use finite buffering by default, including the manual API. With
`W` workers, omitted limits or `None` give each queue capacity `W`: pending input,
terminal results, and the internal worker-result queue. Slow result consumption
blocks further processing and admission.

Set `input_task_queue_size=I` to override the input capacity. A positive value is
a finite item limit; `input_task_queue_size=0` is unlimited.
A supplied `result_queue=asyncio.Queue(R)` sets both result queues to capacity `R`.
It keeps its identity and capacity, including `asyncio.Queue(0)` for unlimited
results. Aqute does not resize or replace it. Helper and manual runs use the same
limits, including after reuse. Published results remain available after cleanup
through `get_result()` or `drain_results()`.

With positive input capacity `I`, result capacity `R`, and one producer, Aqute
retains at most `I + 2*R + W + 3` input/result items, including pending admission,
the collector-held result, and the currently yielded item. The default helper
bound is `4*W + 3` items. The internal worker-result queue uses capacity `R` too.

This is an item-count bound, not a byte limit. It excludes items retained by the
source or caller and the growing list returned by `process_all()`. A queue size of
zero is unlimited. Multiple independent producer calls can each hold an item
while waiting for admission; the formula above assumes one producer.

For manual flows without a concurrent result consumer, see
[manual buffering](#manual-buffering). The
[changelog](https://github.com/insomnes/aqute/blob/main/CHANGELOG.md) describes
migration from the older unlimited defaults.

## Concurrency, rate, and priority

`workers_count` limits occupied workers; it does not specify requests per second.
Set `rate_limiter` to control attempt admission. Every retry acquires it again.
A waiting attempt occupies its worker, including rate-limit and retry-delay waits.
CPU-heavy or blocking handlers block the event loop. For synchronous I/O, see
[thread offloading and its limits](reference.md#synchronous-io-handlers).

Use `TokenBucketRateLimiter(max_rate=N)` to space grants by at least `1 / N`
seconds, with one initial grant available immediately. To pause all workers after
throttling, wrap a limiter in `PausableRateLimiter` and call `pause_for()` inside
the handler before raising a retryable error. Pauses delay admission, leave
already admitted requests running, and can cause delayed grants to resume together.
See [rate limiters](reference.md#rate-limiters) for imports, periods, bursts,
shared pauses, and custom limiters.

With `use_priority_queue=True`, lower `task_priority` values run first among
pending tasks already in the input queue. Priority does not order callers still
waiting for admission or preempt occupied workers. A worker retains its task
through retries. Increasing input capacity lets more tasks compete by priority
but retains more inputs.

## Errors, retries, and timeouts

Handler exceptions become `AquteTask.error` values. `retry_count` specifies extra
attempts; its default is zero. `specific_errors_to_retry` selects exception types.
When omitted, it defaults to `None`: every caught handler error is eligible while
the retry budget remains.
`errors_to_not_retry` excludes types and takes precedence when both filters match.

`retry_delay(failed_attempt, error)` optionally returns finite, nonnegative seconds.
The failed-attempt number is 1-based. The callback runs only when another attempt
is permitted. The default delay is zero. The [retry example](retry_progress.md)
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

Import the public exception types directly from `aqute`:

```python
from aqute import AquteError, AquteTaskTimeoutError, AquteTooManyTasksFailedError
```

`start_timeout_seconds` limits waiting for the first input after start, including
an asynchronous source's first item. `total_failed_tasks_limit` stops processing
with `AquteTooManyTasksFailedError` when collected terminal failures reach the limit.
The limit is inclusive: `total_failed_tasks_limit=1` stops on the first collected
terminal failure, after publishing that task's result. The background run task
raises the error and cancels remaining workers; cancelled handlers do not produce
terminal results.

In the manual API, `finish()` propagates the run error. `get_result()` returns
already-published results first, then propagates the error when the queue is empty
and the run has failed. A blocked `add_task()` also propagates the run error if
the failed run completes before its admission wait returns. Keep failure handling
around the full lifecycle, including [shutdown](#manual-processing-and-shutdown).
Source exceptions propagate after helper cleanup.

The `aqute.worker` logger emits `WARNING` for each handler exception or timeout,
including attempts that will be retried. A warning does not prove terminal failure;
use terminal task outcomes to determine success. To suppress these warnings in
an application that reports outcomes itself:

```python
import logging

logging.getLogger("aqute.worker").setLevel(logging.ERROR)
```

This also suppresses warnings for terminal handler failures. It does not change
retries or result delivery.

The [HTTP recipe](http_client.md) applies these rules to transport errors and HTTP 429.

## Manual processing and shutdown

Use the Aqute async context manager, or call `engine.start()` and later
`await engine.stop()` in a `finally` block. `start()` is synchronous and returns
the background `asyncio.Task`. Awaiting that task waits for the whole run to end;
do not `await engine.start()` before submitting input. The
[manual drain example](manual_drain.md) shows explicit start and cleanup.

Start workers before filling a bounded input queue; otherwise `add_task()` raises
`AquteError` when the input queue is already full. With default limits, submit with
`await add_task(data)` and consume results concurrently with `await get_result()`.
Custom IDs use `task_id`; omitted IDs are generated. `drain_results()` removes currently available results without waiting.
`get_result()` raises `AquteError` after normal completion when no results remain.
While the run is active and the result queue is empty, `get_result()` waits for
a result or run completion, with no timeout of its own.
After start, a full input queue makes `add_task()` wait for capacity;
it raises `AquteError` if the run completes normally before admission.
Default capacity `workers_count` can therefore block service callers
inside submission, so bound concurrent callers or apply an admission timeout.

Results use one shared queue. `get_result()` returns the next available result,
not necessarily the result of the caller's last `add_task()`. For per-caller
responses, use one result consumer and route each task by `task.task_id` to an
application-owned `asyncio.Future`. Register the future under a unique ID before
`await add_task(data, task_id=...)`, because processing can finish during
submission. The application owns cancellation and cleanup of these futures,
including submission failures and shutdown. Check `task.success` or use
`task.unwrap()` when delivering results; a successful result can be `None`.
The runnable [per-caller request pool](request_pool.md) shows this routing pattern,
caller cancellation, and an engine that stays open for later requests.

`finish_submitting()` signals that input submission is complete. `await finish()`
also sends that signal and waits for processing. Stop producers before either
call. These methods do not reject later submissions while the run still has work.
`await run()` starts processing and finishes work submitted before start.
With finite result queues, keep the consumer running while awaiting completion.
For pre-submit-then-run, explicitly set both queues unlimited. For run-then-drain,
set only the result queue unlimited; see [manual buffering](#manual-buffering)
and the [manual drain example](manual_drain.md). After `run()` or `finish()`
completes, await `stop()` before starting a helper.

The executable [service shutdown example](service_shutdown.md) shows:

1. Signal the producer to stop accepting new input.
2. Keep the result consumer running while the producer completes its current submission.
3. Await `finish()` and result draining under `asyncio.timeout`.
4. On deadline expiry, cancel the producer and consumer, then await `stop()`.

`stop()` cancels processing and waits for worker cleanup. It does not drain pending
input. A drain deadline requests cancellation; it cannot force an uncooperative
coroutine to terminate. The example's caller receives whether draining timed out.
In-process completion does not imply durable delivery.
If the run has failed, `stop()` re-raises its error after cleanup, even if another
call already raised it. This also applies to cleanup through the async context
manager or a `finally` block.

Completed results remain available after `stop()`. After `await stop()`, call
`start()` again to reuse the same engine; `stop()` resets the run task and counters.
Drain retained results before using a helper again.

<span id="queue-default-migration"></span>

### Manual buffering

With the default finite queues, keep a result consumer running while submitting
and awaiting `finish()`. Without it, `add_task()` or `finish()` can wait indefinitely;
Aqute does not detect this arrangement.

For run-then-drain, start the engine before submission and set
`result_queue=asyncio.Queue(0)`. The input queue can keep its default capacity.
Results accumulate in memory until drained; see the [manual drain recipe](manual_drain.md).

To submit every input before starting and drain results after completion, make
both queues unlimited:

```python
engine = Aqute(
    handle,
    workers_count=32,
    input_task_queue_size=0,
    result_queue=asyncio.Queue(0),
)
```

A full input queue before `start()` raises `AquteError`; start processing first or
choose unlimited input explicitly. Stop producers before signalling completion.

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
flow to inspect them after `finish()` and before `stop()`. The
[retry](retry_progress.md) and [service](service_shutdown.md) examples log these
snapshots.
