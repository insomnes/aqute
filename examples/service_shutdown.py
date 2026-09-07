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
