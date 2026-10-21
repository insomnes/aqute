# Service shutdown

Stop new input and let the current submission finish while results drain. A
deadline cancels remaining work and waits for cleanup. The returned boolean reports
whether draining timed out if cleanup succeeds. A handler cleanup error raises
`ExceptionGroup`, including when a drain timeout initiated cancellation.
Cancellation is cooperative; the deadline cannot force a coroutine to terminate.
The demonstration uses finite local work.

```bash
uv run --locked python -m examples.service_shutdown
```

<!-- example: examples/service_shutdown.py -->
```python
"""Stop input, drain bounded results under a deadline, then cancel if needed."""

import asyncio
import logging
from collections.abc import Callable, Coroutine
from typing import Any

from aqute import Aqute, AquteError

logger = logging.getLogger(__name__)
SHUTDOWN_AFTER = 3


async def serve(
    handler: Callable[[int], Coroutine[Any, Any, int]],
    *,
    drain_timeout: float = 1.0,
) -> tuple[list[int], bool]:
    engine = Aqute(handler, 2, input_task_queue_size=2, result_queue=asyncio.Queue(2))
    stop_input = asyncio.Event()
    ready = asyncio.Event()
    production_done = asyncio.Event()
    submitted = 0
    values: list[int] = []
    timed_out = False

    async def produce() -> None:
        nonlocal submitted
        try:
            job_id = 0
            while not stop_input.is_set():
                await engine.add_task(job_id)
                submitted += 1
                job_id += 1
                if submitted >= SHUTDOWN_AFTER:
                    ready.set()
        finally:
            production_done.set()

    async def collect() -> None:
        while not production_done.is_set() or len(values) < submitted:
            try:
                task = await engine.get_result()
            except AquteError:
                if production_done.is_set() and len(values) == submitted:
                    return
                raise
            if task.error is not None:
                raise task.error
            assert task.result is not None
            values.append(task.result)
            logger.info("Progress: %s", engine.counters)

    async with engine, asyncio.TaskGroup() as group:
        producer = group.create_task(produce())
        collector = group.create_task(collect())
        # In a service, wait for its shutdown signal here.
        await ready.wait()
        stop_input.set()
        try:
            async with asyncio.timeout(drain_timeout):
                await producer  # Complete any submission already in flight.
                await engine.finish()
                await collector  # Results must drain while finish() waits.
        except TimeoutError:
            timed_out = True
            producer.cancel()
            collector.cancel()
            await engine.stop()
    return values, timed_out


async def main() -> list[int]:
    async def handle(job_id: int) -> int:
        await asyncio.sleep(0.001)
        return job_id

    values, timed_out = await serve(handle)
    assert not timed_out
    assert sorted(values) == list(range(len(values)))
    assert len(values) >= SHUTDOWN_AFTER
    return values


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("Results: %s", sorted(asyncio.run(main())))
```
<!-- /example -->

## When handler cleanup fails

In 0.11.0, an ordinary exception raised by a handler during cancellation cleanup
propagates in an `ExceptionGroup`. Aqute waits for cooperative worker cleanup
before raising it. The failed invocation is not retried and does not produce an
ordinary task result. An expired drain deadline can therefore raise a cleanup
error group instead of `TimeoutError`.

This example starts one handler, then uses a zero-second drain deadline to
cancel it. The handler simulates a resource cleanup failure. The outer
`except* OSError` logs matching errors, including errors nested inside groups,
then re-raises them so the caller can report failed shutdown.

```bash
uv run --locked python -m examples.shutdown_failure
```

The command deliberately exits with a nonzero status. Its log and traceback
include `Handler cleanup failed during shutdown` and
`OSError: remote resource cleanup failed`.

<!-- example: examples/shutdown_failure.py -->
```python
"""Log and propagate a handler cleanup failure after a drain deadline."""

import asyncio
import logging

from aqute import Aqute

logger = logging.getLogger(__name__)


async def main() -> None:
    started = asyncio.Event()

    async def handle(job_id: int) -> int:
        try:
            started.set()
            await asyncio.Event().wait()
            return job_id
        finally:
            # Simulate a failure while closing the handler's external resource.
            raise OSError("remote resource cleanup failed")

    engine = Aqute(handle, 1)
    try:
        async with engine:
            await engine.add_task(1)
            await started.wait()
            # An immediate drain deadline cancels the already-running handler.
            async with asyncio.timeout(0):
                await engine.finish()
    except* OSError:
        logger.exception("Handler cleanup failed during shutdown")
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
```
<!-- /example -->

In application code, catch only errors for which you have an explicit recovery
policy. Logging an error does not make resource cleanup successful. If cleanup
succeeds, caller cancellation still propagates as `CancelledError`, and a drain
deadline still raises `TimeoutError`.

See [usage](usage.md) for the complete lifecycle and error contracts.
