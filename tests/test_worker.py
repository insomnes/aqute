import asyncio
from typing import Any

import pytest

from aqute.task import END_MARKER, AquteTask, AquteTaskQueueType
from aqute.worker import Foreman, Worker


async def sample_handler(data) -> str:
    await asyncio.sleep(0.01)
    return f"handled-{data}"


async def failing_handler(_: Any) -> None:
    raise ValueError("Oops")


@pytest.mark.asyncio
async def test_worker_handle_task():
    input_q: AquteTaskQueueType = asyncio.Queue()
    output_q: AquteTaskQueueType = asyncio.Queue()

    worker = Worker(
        name="TestWorker",
        handle_coro=sample_handler,
        input_q=input_q,
        output_q=output_q,
    )

    task = AquteTask(data="task_data", task_id="1")
    await worker.handle_task(task)

    out_task = await output_q.get()
    assert out_task.result == "handled-task_data"


@pytest.mark.asyncio
async def test_worker_handle_task_error():
    input_q: AquteTaskQueueType = asyncio.Queue()
    output_q: AquteTaskQueueType = asyncio.Queue()

    worker = Worker(
        name="TestWorker",
        handle_coro=failing_handler,
        input_q=input_q,
        output_q=output_q,
    )

    task = AquteTask(data="task_data", task_id="1")
    await worker.handle_task(task)

    out_task = await output_q.get()
    assert out_task.result is None
    assert not out_task.success
    assert isinstance(out_task.error, ValueError)


@pytest.mark.asyncio
async def test_handle_task_end():
    input_q: AquteTaskQueueType = asyncio.Queue()
    output_q: AquteTaskQueueType = asyncio.Queue()

    worker = Worker(
        name="TestWorker",
        handle_coro=sample_handler,
        input_q=input_q,
        output_q=output_q,
    )

    run_aiotask = asyncio.create_task(worker.run())

    # Test that some object() do not stop us
    task_with_object_data = AquteTask(object(), "pseudo-finish")
    await worker.input_q.put(task_with_object_data)
    out_task = await worker.output_q.get()
    assert out_task is task_with_object_data

    # And now we can check proper finish
    finish_task = AquteTask(END_MARKER, "real-finish")
    await worker.input_q.put(finish_task)
    await run_aiotask
    assert worker.output_q.empty()


@pytest.mark.asyncio
async def test_foreman_workflow():
    foreman = Foreman(
        handle_coro=sample_handler,
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
        handle_coro=sample_handler,
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
