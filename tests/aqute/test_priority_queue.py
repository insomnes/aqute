import asyncio

import pytest

from aqute import Aqute


async def non_failing_handler(i: int) -> str:
    await asyncio.sleep(0.01)
    return f"Success {i}"


@pytest.mark.asyncio
async def test_priority_queue():
    aqute = Aqute(
        workers_count=1,
        handle_coro=non_failing_handler,
        use_priority_queue=True,
        input_task_queue_size=0,
        result_queue=asyncio.Queue(0),
    )
    await aqute.add_task(1_000_000)
    await aqute.add_task(10, task_priority=10)
    await aqute.add_task(5, task_priority=5)
    await aqute.add_task(10, task_priority=10)
    await aqute.add_task(1, task_priority=1)

    async with aqute:
        await aqute.finish()

    results = aqute.drain_results()
    assert [t.data for t in results] == [1, 5, 10, 10, 1_000_000]
