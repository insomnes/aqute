# Manual result draining

Use the manual API to submit custom task IDs and priorities. If those features
are unnecessary, prefer `process_all()` for an ordered result list.

Start the engine before submitting, then wait for completion and drain results.
This run-then-drain flow must use `result_queue=asyncio.Queue(0)` because no
consumer runs during processing. The input queue keeps its default capacity of
`workers_count`. Results accumulate in memory until drained.

For bounded result buffering, consume `get_result()` concurrently instead; see
[service shutdown](service_shutdown.md). To submit all inputs before starting,
set `input_task_queue_size=0` as well. See [queue-default migration](usage.md#queue-default-migration).

Lower priorities run first among pending tasks. They do not preempt active
workers. The example sorts the returned values by task ID and runs offline.

```bash
uv run --locked python -m examples.manual_drain
```

```python
--8<-- "examples/manual_drain.py"
```
