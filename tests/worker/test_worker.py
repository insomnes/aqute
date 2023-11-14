import asyncio
from typing import Any

import pytest

from aqute.errors import AquteTaskTimeoutError
from aqute.task import END_MARKER, AquteTask, AquteTaskQueueType
from aqute.worker import Worker


async def simple_handler(data) -> str:
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
        handle_coro=simple_handler,
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
        handle_coro=simple_handler,
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
    assert worker.input_q.empty()


@pytest.mark.asyncio
async def test_worker_handle_task_timeout():
    input_q: AquteTaskQueueType = asyncio.Queue()
    output_q: AquteTaskQueueType = asyncio.Queue()

    task_timeout = 0.01

    async def slow_handler(data: str) -> str:
        await asyncio.sleep(task_timeout * 2)
        return f"handled-{data}"

    worker = Worker(
        name="TestWorker",
        handle_coro=slow_handler,
        input_q=input_q,
        output_q=output_q,
        task_timeout_seconds=task_timeout,
    )

    task = AquteTask(data="task_data", task_id="1")
    await worker.handle_task(task)

    out_task = await output_q.get()
    assert out_task.error is not None
    assert isinstance(out_task.error, AquteTaskTimeoutError)
