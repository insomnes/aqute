# Worker queues

Import `Foreman` from `aqute.worker`. Prefer [`Aqute`](engine.md) for managed
results, retries, priority configuration, and progress snapshots.

`Foreman` accepts [`AquteTask`](tasks.md#aqute.AquteTask) objects directly.
Inspect `error` and `result` on returned tasks: `Foreman` does not set `success`,
so `unwrap()` cannot return successful worker results.

Consume results while waiting for `finalize()`, especially with a finite output
queue. Both `finalize()` and `stop()` reset the worker queues; retrieve results
before cleanup completes. See the
[worker queue behavior guide](../reference.md#lower-level-worker-queues).

::: aqute.worker.Foreman
    options:
      members:
        - start
        - add_task
        - get_handled_task
        - finalize
        - stop
