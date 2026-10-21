# Changelog

## 0.11.0 - 2026-10-21

- Add Python 3.15 support to the Python 3.11–3.15 CI and installed-wheel checks.
  Python 3.11 remains the minimum supported version.
- Fix shutdown hangs when a handler raises an ordinary exception during
  cancellation cleanup. Propagate the failure in `ExceptionGroup` after
  cooperative worker cleanup, without retrying the invocation or returning it
  as an ordinary task result.
- Preserve ordinary handler failure and timeout retry behavior, including
  failures from a handler's own `asyncio.TaskGroup`.
- Add a shutdown-failure example and clarify cleanup error handling.

### Upgrading from 0.10.x

Shutdown can now raise `ExceptionGroup` when handler cleanup fails, including
when caller cancellation or a drain deadline initiated shutdown. Code that
catches only `CancelledError` or `TimeoutError` must also allow cleanup errors
to propagate, or handle selected errors with `except*` under an explicit recovery
policy. See the [shutdown-failure example](
https://insomnes.github.io/aqute/service_shutdown/#when-handler-cleanup-fails).

## 0.10.4 - 2026-10-20

- Add a generated [API reference](https://insomnes.github.io/aqute/api/) for the
  engine, tasks and counters, rate limiters, exceptions, and worker queues.
- Complete public API docstrings and clarify worker result and cleanup limits.
  Keep the handwritten behavior guide and its existing anchors.
- Add a resumable batch recipe with application-owned checkpoints.

## 0.10.3 - 2026-09-09

- Raise `AquteError` from `finish()` and `iter_results()` when processing is
  independently cancelled. Preserve caller cancellation and cleanup deadlines.
- Process prequeued inputs with zero or negative `start_timeout_seconds`;
  the timeout applies only while waiting for the first input.
- Treat `specific_errors_to_retry=()` as selecting no retryable errors.
- Remove the unused, importable `aqute.task.END_MARKER` sentinel and its worker
  shutdown path.
- Clarify that cancellation of `Aqute.add_task()` or `Foreman.add_task()` can
  race completed admission and does not prove that the task was not accepted.

## 0.10.2 - 2026-09-09

- Keep generated task IDs distinct during concurrent submissions waiting for input
  capacity. Cancelled submissions can leave gaps in the per-run sequence.
- Preserve handler-raised `TimeoutError` when the Aqute deadline has not expired,
  including with no deadline. Previously, Aqute converted it to
  `AquteTaskTimeoutError`; retry filters now match the original exception.
  Use `TimeoutError` for handler timeouts and `AquteTaskTimeoutError` for Aqute
  deadlines, or include both types to select or exclude both kinds of timeout.
- Fail the run with an `ExceptionGroup` containing `AquteError` when an awaited
  operation propagates `asyncio.CancelledError` without a cancellation request
  on the worker, instead of leaving the run waiting indefinitely. The cancelled
  work is not retried and produces no terminal task result. Sibling workers are
  cancelled and awaited for cooperative cleanup; stop and caller cancellation
  retain their existing behavior.

## 0.10.1 - 2026-09-09

- Fix cancelled submissions to a full input queue leaving later results
  unpublished. Admission and its task count now complete together, so cancelling
  the submitting caller does not lose track of work already queued.

## 0.10.0 - 2026-09-08

### Breaking changes from 0.9.2

Python 3.11 or newer is required. The
[0.9.x maintenance branch](https://github.com/insomnes/aqute/tree/maintenance/0.9.x)
retains Python 3.9/3.10 support and the old method names.

The public API uses one method set. The old names are removed without
compatibility wrappers; rename calls during the upgrade:

| 0.9.2 call | Replacement |
| --- | --- |
| `engine.set_all_tasks_added()` | `engine.finish_submitting()` |
| `await engine.wait_till_end()` | `await engine.finish()` |
| `await engine.start_and_wait()` | `await engine.run()` |
| `await engine.get_task_result()` | `await engine.get_result()` |
| `engine.extract_all_results()` | `engine.drain_results()` |
| `await engine.apply_to_all(items)` | `await engine.process_all(items)` |
| `engine.apply_to_each(items)` | `async with engine.iter_results(items) as results` |

Streaming requires an async context. Iterate its yielded iterator inside that
context so exit awaits producer and worker cleanup, including after early exit:

```python
async with engine.iter_results(items) as results:
    async for task in results:
        consume(task)
```

Helper and manual runs now use finite input and result queues by default, each
with capacity `workers_count`. Omitted limits and `None` use this capacity;
positive limits and explicit zero keep their meaning. Manual runs must consume
results concurrently or explicitly choose unlimited buffering. For
pre-submit-then-run with results drained after completion, set both
`input_task_queue_size=0` and `result_queue=asyncio.Queue(0)`. For run-then-drain,
only the result queue must be unlimited. Before processing starts, `add_task()`
raises `AquteError` when the input queue is full. With finite result queues and no
concurrent consumer, submission or completion can wait indefinitely. See
[manual buffering](https://insomnes.github.io/aqute/usage/#manual-buffering).

`process_all()` still retains the complete result list. Helpers reject pending
manual tasks and unconsumed results; finish manual work and drain results before
reusing a stopped engine, or use a fresh engine. See
[buffering and migration](https://insomnes.github.io/aqute/usage/#buffering).
