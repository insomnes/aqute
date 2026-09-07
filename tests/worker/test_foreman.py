import asyncio

import pytest

from aqute.errors import AquteTaskTimeoutError
from aqute.task import AquteTask
from aqute.worker import Foreman


async def simple_handler(data) -> str:
    await asyncio.sleep(0.01)
    return f"handled-{data}"


@pytest.mark.asyncio
async def test_foreman_workflow():
    foreman = Foreman(
        handle_coro=simple_handler,
        workers_count=1,
    )

    task = AquteTask(data="task_data", task_id="1")
    await foreman.add_task(task)

    foreman.start()

    await asyncio.sleep(0.1)
    handled_task = await foreman.get_handled_task()
    await foreman.finalize()

    assert handled_task.result == "handled-task_data"


@pytest.mark.asyncio
async def test_foreman_finalize():
    foreman = Foreman(
        handle_coro=simple_handler,
        workers_count=1,
    )

    task1 = AquteTask(data="task_data1", task_id="1")
    task2 = AquteTask(data="task_data2", task_id="2")
    await foreman.add_task(task1)
    await foreman.add_task(task2)

    foreman.start()

    handled_task1 = await foreman.get_handled_task()
    handled_task2 = await foreman.get_handled_task()

    assert handled_task1.result == "handled-task_data1"
    assert handled_task2.result == "handled-task_data2"

    await foreman.finalize()
    assert foreman.in_queue.empty()
    foreman.start()
    await foreman.add_task(AquteTask(data="reused", task_id="3"))
    assert (await foreman.get_handled_task()).result == "handled-reused"
    await foreman.finalize()


@pytest.mark.asyncio
async def test_timeout():
    task_timeout = 0.06

    async def slow_handler(data: int) -> str:
        await asyncio.sleep(0.01 * data)
        return f"handled-{data}"

    foreman = Foreman(
        handle_coro=slow_handler,
        workers_count=2,
        task_timeout_seconds=task_timeout,
    )

    for i in range(1, 11):
        task = AquteTask(data=i, task_id=str(i))
        await foreman.add_task(task)

    foreman.start()

    handled_tasks = [await foreman.get_handled_task() for _ in range(10)]

    errors = [t.error for t in handled_tasks if t.error is not None]
    assert len(errors) == 5
    assert all([isinstance(e, AquteTaskTimeoutError) for e in errors])

    successful_tasks = [t for t in handled_tasks if t.error is None]
    assert len(successful_tasks) == 5

    await foreman.finalize()
    assert foreman.in_queue.empty()


@pytest.mark.asyncio
async def test_finalize_drains_all_priorities():
    handled = []

    async def handler(value: int) -> int:
        handled.append(value)
        return value

    foreman = Foreman(handler, 1, use_priority_queue=True)
    await foreman.add_task(AquteTask(1, "1", _priority=2_000_000))
    await foreman.add_task(AquteTask(2, "2", _priority=3_000_000))
    foreman.start()
    async with asyncio.timeout(1):
        await foreman.finalize()
    assert handled == [1, 2]


@pytest.mark.asyncio
async def test_stop_waits_for_all_handler_cleanup():
    started = asyncio.Queue()
    cleaned = []

    async def handler(value: int) -> int:
        await started.put(value)
        try:
            await asyncio.Event().wait()
            return value
        finally:
            await asyncio.sleep(0)
            cleaned.append(value)

    foreman = Foreman(handler, 2)
    for value in range(2):
        await foreman.add_task(AquteTask(value, str(value)))
    foreman.start()
    async with asyncio.timeout(1):
        for _ in range(2):
            await started.get()
        await foreman.stop()
    assert sorted(cleaned) == [0, 1]
