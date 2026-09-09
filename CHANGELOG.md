# Changelog

## Unreleased

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
