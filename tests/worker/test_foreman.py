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
    worker_aiotasks = [wt for wt in foreman._worker_jobs]

    handled_task1 = await foreman.get_handled_task()
    handled_task2 = await foreman.get_handled_task()

    assert handled_task1.result == "handled-task_data1"
    assert handled_task2.result == "handled-task_data2"

    await foreman.finalize()
    assert foreman._worker_jobs == []
    assert all([wt.done() for wt in worker_aiotasks])


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
    worker_aiotasks = [wt for wt in foreman._worker_jobs]

    handled_tasks = [await foreman.get_handled_task() for _ in range(10)]

    errors = [t.error for t in handled_tasks if t.error is not None]
    assert len(errors) == 5
    assert all([isinstance(e, AquteTaskTimeoutError) for e in errors])

    successful_tasks = [t for t in handled_tasks if t.error is None]
    assert len(successful_tasks) == 5

    await foreman.finalize()
    assert foreman._worker_jobs == []
    assert all([wt.done() for wt in worker_aiotasks])
