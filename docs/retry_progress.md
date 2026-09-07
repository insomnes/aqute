# Retry backoff and progress

After the [streaming introduction](streaming.md), use this finite example to add
selected retries and progress snapshots. One handler call fails once, then
succeeds after capped exponential backoff with jitter. The HTTP example imports
the same `retry_delay` callback.

```bash
uv run --locked python -m examples.retry_progress
```

```python
--8<-- "examples/retry_progress.py"
```

`retry_count=2` permits two extra attempts. This example selects `ValueError`;
applications must select safe retry errors and operations for their own handlers.
See [retry rules](usage.md#errors-retries-and-timeouts) for delay and error contracts.

Read `engine.counters` during iteration. Helpers stop on context exit, which resets
the counters. See [progress counters](usage.md#progress-counters) for each field.

This small example collects eight results to verify its output. Its result list
and per-input attempt counts grow with the input length, outside Aqute's queue
bounds. Use the introductory recipe's incremental consumption for larger inputs.
