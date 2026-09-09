import asyncio
import contextlib
from typing import Any

import pytest

from aqute.errors import AquteTaskTimeoutError
from aqute.task import AquteTask, AquteTaskQueueType
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
async def test_worker_processes_object_payload_and_cleans_up_on_cancellation():
    """Object payloads reach the handler; cancellation awaits active task cleanup."""
    input_q: AquteTaskQueueType = asyncio.Queue()
    output_q: AquteTaskQueueType = asyncio.Queue()
    started = asyncio.Event()
    cleaned = asyncio.Event()
    payload = object()

    async def handler(data):
        if data is payload:
            return "handled object"
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0)
            cleaned.set()

    worker = Worker(
        name="TestWorker",
        handle_coro=handler,
        input_q=input_q,
        output_q=output_q,
    )
    run_aiotask = asyncio.create_task(worker.run())
    try:
        async with asyncio.timeout(1):
            await input_q.put(AquteTask(payload, "object"))
            out_task = await output_q.get()
            assert out_task.data is payload
            assert out_task.result == "handled object"
            await input_q.put(AquteTask("wait", "cancelled"))
            await started.wait()
            run_aiotask.cancel()
            with pytest.raises(asyncio.CancelledError):
                await run_aiotask
            await input_q.join()
        assert cleaned.is_set()
        assert output_q.empty()
    finally:
        run_aiotask.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await run_aiotask


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
