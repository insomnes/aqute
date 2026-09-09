# Usage

This page covers the 0.10.0 API for Python 3.11+. Follow the
[installation instructions](index.md#installation).

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
consuming input. Use the manual API for externally managed producers and consumers.

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

### Queue-default migration

This is a pre-1.0 behavior break for manual runs. Omitted limits now use capacity
`workers_count`, just like helper runs. Existing positive capacities and explicit
zero limits keep their meaning. Manual runs using the defaults must consume
results concurrently with submission and processing.

To preserve pre-submit-then-run or other unlimited buffering, set both limits
explicitly:

```python
engine = Aqute(
    handle,
    workers_count=32,
    input_task_queue_size=0,
    result_queue=asyncio.Queue(0),
)
```

Pre-submit-then-run needs both queues unlimited when all inputs are submitted
before starting and results are drained after completion. A full input queue
before `start()` now raises `AquteError` from `add_task()` instead of waiting
indefinitely. Start processing first or opt into unlimited input explicitly.

Run-then-drain starts processing before submission and needs only
`result_queue=asyncio.Queue(0)`; the input queue can keep its default capacity.
See the [manual drain example](manual_drain.md). With finite result queues and no
concurrent consumer, `add_task()` or `finish()` can wait indefinitely. Aqute does
not detect this arrangement.

Helpers now reject pending manual tasks, so preloading with `add_task()` before
`process_all()` or `iter_results()` raises `AquteError`. Follow
[manual processing and shutdown](#manual-processing-and-shutdown) to finish the
work while draining results, then stop the engine before using a helper.
Alternatively, use a separate fresh engine for the helper.

## Concurrency, rate, and priority

`workers_count` limits occupied workers. It does not specify requests per second.
Use `rate_limiter` to control attempt rate. Every retry acquires the limiter again.
Available implementations are in `aqute.ratelimiter`:

- `TokenBucketRateLimiter` controls a shared rate, with optional bursts.
- `SlidingRateLimiter` limits calls within a moving time window.
- `PerWorkerRateLimiter` applies a separate token bucket to each worker.
- `RandomizedIntervalRateLimiter` adds bounded random delays after rolling-cap waits.
- `PausableRateLimiter` wraps any limiter with a shared monotonic pause.

For `TokenBucketRateLimiter` and `SlidingRateLimiter`, `max_rate` applies over
`time_period` seconds; the default period is one second. To allow five grants
in each rolling 0.2-second window:

```python
from aqute.ratelimiter import SlidingRateLimiter

limiter = SlidingRateLimiter(max_rate=5, time_period=0.2)
```

The sliding limiter can grant all five permits together. A token bucket with
`allow_burst=False` (the default) instead spaces grants by
`time_period / max_rate` seconds.
These limits apply to limiter grants; a pause wrapper can delay handler starts
after a grant, as described below.

To pause attempt admission after service throttling, share one wrapper:

```python
from aqute.ratelimiter import PausableRateLimiter, TokenBucketRateLimiter

limiter = PausableRateLimiter(TokenBucketRateLimiter(max_rate=10))
engine = Aqute(handle, workers_count=4, rate_limiter=limiter)
# Inside handle(), before raising a retryable throttle error:
limiter.pause_for(2.0)
```

Call `pause_for()` in the handler before raising the retryable throttle error;
the result consumer receives only terminal outcomes and cannot pause earlier retries.

`pause_for(seconds)` extends the pause from `time.monotonic()` now.
`pause_until(deadline)` takes an absolute deadline from that same clock.
Both accept finite, nonnegative values and raise `ValueError` otherwise.
The later deadline wins; a shorter pause cannot shorten the current one.
Zero seconds and past deadlines add no wait. The read-only `paused_until`
property starts at zero and retains the latest deadline after it expires.

Use the wrapper on one event loop. Each `acquire()` checks the deadline in a loop
before and after acquiring the inner limiter, including extensions set while
either wait is active. Cancellation propagates. Requests already admitted are
not interrupted. Inner grants delayed by a pause can resume together; the wrapper
does not reacquire their permits or guarantee request spacing after that delay.

The pause is outside `task_timeout_seconds`. A throttled attempt still consumes
`retry_count`; select retryable errors and allow enough retries in the application.
SDK-internal retries inside the handler multiply requests without acquiring the
limiter again. The [HTTP example source](http_client.md#runnable-source) parses
`Retry-After` seconds or HTTP dates in application code and applies the shared
pause. Missing or invalid headers use a one-second fallback; past dates use zero.
The example applies no upper bound to valid `Retry-After` delays. Apply any
required delay cap in application code.

Its offline entry point also compares eight workers processing forty URLs during
one 300 ms throttle window, with `Retry-After: 1` and a 100-attempt/second limiter.
The transport returns each response immediately, before the next admission.
It logs throttled-request counts, retries, and elapsed time with and without the
shared pause. Real requests already in flight can still return 429 after a pause;
this controlled comparison is not a bound on their count or a throughput claim.

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

For a synchronous I/O handler, wrap the call in an async handler with
`return await asyncio.to_thread(fn, item)`. The default executor caps its threads
at `min(32, (cpu_count or 1) + 4)`: Python 3.11–3.12 uses `os.cpu_count()`, while
Python 3.13+ uses `os.process_cpu_count()`. Effective concurrency can therefore be
below `workers_count`. If needed, configure a custom `ThreadPoolExecutor(max_workers=...)`
with `asyncio.get_running_loop().set_default_executor(executor)` before submitting
work, and own its shutdown. See Python's [executor defaults](https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor)
and [`to_thread`](https://docs.python.org/3/library/asyncio-task.html#asyncio.to_thread).
Cancellation or an Aqute timeout does not stop an already running thread; retries
can overlap the original call, so use operation-level timeouts and safe retry
policies. Threads generally do not make CPU-bound Python work parallel under the GIL.

With `use_priority_queue=True`, pass `task_priority` to `add_task()`. Lower values
run first among admitted pending tasks. Priority does not preempt an occupied
worker. A worker retains its task through retries.
Only tasks currently in the input queue can be reordered. A positive
`input_task_queue_size` bounds this set; the default capacity is `workers_count`.
Increasing capacity lets more pending tasks compete by priority but retains more
inputs. Priority does not order callers still waiting for admission.

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

The [HTTP example](http_client.md) uses one shared HTTPX client and retries
transport errors and HTTP 429 responses. Other HTTP status failures propagate
to its caller. Its executable
entry point uses an offline transport; applications call `fetch_pages(urls)` for
real requests. HTTPX is required only for this example and is included in the dev
group. See [HTTPX's async client guide](https://www.python-httpx.org/async/).

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
set only the result queue unlimited; see [queue-default migration](#queue-default-migration)
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
Drain retained results before using a helper again. For lower-level worker queues,
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
flow to inspect them after `finish()` and before `stop()`. The
[retry](retry_progress.md) and [service](service_shutdown.md) examples log these
snapshots.

## Migration from 0.9.2

The old method names are removed without compatibility wrappers. Update calls
when upgrading from 0.9.2:

| 0.9.2 call | Replacement |
| --- | --- |
| `engine.set_all_tasks_added()` | `engine.finish_submitting()` |
| `await engine.wait_till_end()` | `await engine.finish()` |
| `await engine.start_and_wait()` | `await engine.run()` |
| `await engine.get_task_result()` | `await engine.get_result()` |
| `engine.extract_all_results()` | `engine.drain_results()` |
| `await engine.apply_to_all(items)` | `await engine.process_all(items)` |
| `engine.apply_to_each(items)` | `async with engine.iter_results(items) as results` |

For streaming, iterate `results` inside the async context shown in
[streaming and cleanup](#streaming-and-cleanup). The context awaits cleanup on
exit, including after an early `break`. Call `anext(results)` on the yielded
iterator; do not call `aclose()` on the context.

`start()`, `stop()`, and `add_task()` keep their names. Generic handler input and
result types are preserved through the public methods and the async context
manager. For example, a handler accepting `int` makes `add_task("text")` a type
error; this is an intentionally invalid call, not a runnable usage example.

The upgrade also requires Python 3.11 or newer and uses finite buffering by
default for helper and manual runs. See [buffering](#buffering) for explicit unlimited settings and the
[changelog](https://github.com/insomnes/aqute/blob/main/CHANGELOG.md) for the
breaking changes together. The
[0.9.x maintenance branch](https://github.com/insomnes/aqute/tree/maintenance/0.9.x)
retains the old API for Python 3.9 and 3.10.

## For coding agents

For application code, [install Aqute](index.md#installation)
with Python 3.11+ and import `Aqute` from `aqute`. The
[canonical HTTP recipe](http_client.md) also requires `httpx`; the checkout's dev
group includes it. For independent callers sharing a long-lived engine, start
with the [per-caller request pool](request_pool.md). Choose the example matching
the application's input and result flow instead of reconstructing the API.

| Application need | API |
| --- | --- |
| A finite batch with an ordered result list | `await engine.process_all(items)` |
| Results in completion order, with bounded buffering and incremental consumption | `async with engine.iter_results(items) as results`, then iterate `results` inside the context |
| Continuous submission with separate producer and consumer ownership | Follow [manual processing and shutdown](#manual-processing-and-shutdown) |
| Independent callers each waiting for their own reply from a shared engine | Use the [per-caller request pool](request_pool.md) example |

- Create a fresh engine for each helper run. The stream context waits for cleanup
  after completion, early exit, or failure. Keep the external client open around
  that context. Aqute closes started generator sources; callers own other source
  resources and must close them explicitly.
- Leave queue limits omitted for finite defaults in all APIs, with each queue
  sized to `workers_count`. Use positive capacities to override them; zero is unlimited.
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
