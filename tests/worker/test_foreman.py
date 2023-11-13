import asyncio

import pytest

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
    handled_task = await foreman.get_handeled_task()
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

    handled_task1 = await foreman.get_handeled_task()
    handled_task2 = await foreman.get_handeled_task()

    assert handled_task1.result == "handled-task_data1"
    assert handled_task2.result == "handled-task_data2"

    await foreman.finalize()
    assert foreman._worker_jobs == []
    assert all([wt.done() for wt in worker_aiotasks])
