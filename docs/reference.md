# API and rate limits

This reference covers Aqute 0.10.3 for Python 3.11+.
For runnable workflows, start with [Choose an API](index.md#choose-an-api) and the [usage guide](usage.md).
Import the engine with `from aqute import Aqute`.

## Constructor

`Aqute(handle_coro, workers_count, ...)` takes two required arguments.
All remaining parameters are keyword-only. `TData` and `TResult` are the handler's
input and output types; public methods preserve these types.

| Parameter | Type | Default | Meaning |
| --- | --- | --- | --- |
| `handle_coro` | `Callable[[TData], Coroutine[Any, Any, TResult]]` | Required | Async handler called with one input item per attempt. |
| `workers_count` | `int` | Required | Maximum occupied workers, including rate-limit and retry waits. |
| `rate_limiter` | `RateLimiter` or `None` | `None` | Attempt admission limiter; retries acquire it again. |
| `result_queue` | `asyncio.Queue[AquteTask[TData, TResult]]` or `None` | `None` | Supplied result queue; its capacity also limits the internal result relay. Omission gives each capacity `workers_count`. |
| `retry_count` | `int` | `0` | Extra attempts after the first handler call. |
| `retry_delay` | `Callable[[int, Exception], float]` or `None` | `None` | Delay in seconds before a permitted retry; omission means zero delay. |
| `specific_errors_to_retry` | Exception class, tuple of exception classes, or `None` | `None` | Eligible handler errors; `None` selects all caught handler errors while retry budget remains; `()` selects none. |
| `errors_to_not_retry` | Exception class, tuple of exception classes, or `None` | `None` | Excluded errors; takes precedence over the retry selection. |
| `start_timeout_seconds` | `int`, `float`, or `None` | `None` | Limit on waiting for the first input after start; `None` disables the limit; prequeued inputs need no wait, even at zero or negative values. |
| `input_task_queue_size` | `int` or `None` | `None` | Pending input capacity; `None` means `workers_count`, zero means unlimited. |
| `use_priority_queue` | `bool` | `False` | Order admitted pending tasks by increasing `task_priority`. |
| `task_timeout_seconds` | `int`, `float`, or `None` | `None` | Timeout per handler attempt, excluding limiter and retry waits; `None` disables it. |
| `total_failed_tasks_limit` | `int` or `None` | `None` | Stop when collected terminal failures reach this inclusive limit; `None` disables it. |

Queue limits count items, not bytes or caller-retained data. See
[buffering](usage.md#buffering) for the complete bound and
[errors, retries, and timeouts](usage.md#errors-retries-and-timeouts) for failure behavior.

## Helper options

`process_all(items, *, submission_batch_size=1)` returns an awaited list in input
order. `iter_results(items, *, submission_batch_size=1)` returns an async context
for results in completion order. Both accept `Iterable[TData]` and
`AsyncIterable[TData]`. `submission_batch_size` is a positive integer; larger
batches can improve throughput for small tasks at the cost of result latency.
Handlers still receive one item per call. See [streaming cleanup](usage.md#streaming-and-cleanup).

## Rate limiters

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

### Shared pauses

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
limiter again. The [HTTP recipe](http_client.md#limits-and-outcomes) parses
`Retry-After` seconds or HTTP dates in application code and applies the shared
pause. Missing or invalid headers use a one-second fallback; past dates use zero.
The example applies no upper bound to valid `Retry-After` delays. Apply any
required delay cap in application code.

### Randomized intervals

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

### Custom limiters

A custom limiter implements `async acquire(name="", task=None)`. It must propagate
cancellation. CPU-heavy or blocking work in a handler blocks the event loop;
Aqute does not move it to threads or processes automatically.

## Synchronous I/O handlers

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

## Lower-level worker queues

For lower-level worker queues,
`aqute.worker.Foreman` exposes `start()`, `add_task(AquteTask(...))`,
`get_handled_task()`, `finalize()`, and `stop()`. Consume finite result queues while
waiting for `finalize()`.

Cancellation of `Foreman.add_task()` can race completed queue admission. It does
not prove that the task was not accepted or prevent its processing.

Inspect `error` and `result` on tasks returned directly by `Foreman`. It does not
set `success`; `unwrap()` requires the engine's terminal success flag to return
a value.
