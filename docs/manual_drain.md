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

<!-- example: examples/manual_drain.py -->
```python
"""Submit custom task IDs and priorities, then drain results after completion."""

import asyncio
import logging

from aqute import Aqute


async def handle(value: int) -> int:
    return value * 2


async def main() -> list[tuple[str, int]]:
    # Use process_all() when custom task IDs and priorities are unnecessary.
    engine = Aqute(
        handle,
        workers_count=2,
        use_priority_queue=True,
        # Results must be unlimited because consumption starts after finish().
        result_queue=asyncio.Queue(0),
    )
    engine.start()  # Synchronous: awaiting this task would wait for the whole run.
    try:
        for value in range(10):
            await engine.add_task(
                value, task_id=f"job-{value}", task_priority=10 - value
            )
        await engine.finish()
    finally:
        await engine.stop()

    # Priority applies to pending tasks; it does not preempt active workers.
    return sorted((task.task_id, task.unwrap()) for task in engine.drain_results())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("Results: %s", asyncio.run(main()))
```
<!-- /example -->
