import asyncio
import contextlib

import pytest

from aqute import Aqute, AquteError, AquteTaskTimeoutError, AquteTooManyTasksFailedError


async def echo(value: int) -> int:
    return value


@pytest.mark.asyncio
async def test_wait_after_all_results_were_consumed():
    async with Aqute(echo, 1) as engine:
        await engine.add_task(7)
        assert (await engine.get_task_result()).result == 7
        async with asyncio.timeout(1):
            await engine.wait_till_end()
        with pytest.raises(AquteError, match="without another task result"):
            await engine.get_task_result()


@pytest.mark.asyncio
async def test_wait_after_starting_empty_load():
    async with Aqute(echo, 1) as engine:
        await asyncio.sleep(0)
        async with asyncio.timeout(1):
            await engine.wait_till_end()
    assert engine.extract_all_results() == []


@pytest.mark.asyncio
async def test_cancel_load_waits_for_handler_cleanup():
    started = asyncio.Event()
    cleaned = asyncio.Event()

    async def handler(value: int) -> int:
        started.set()
        try:
            await asyncio.Event().wait()
            return value
        finally:
            await asyncio.sleep(0)
            cleaned.set()

    engine = Aqute(handler, 1)
    await engine.add_task(1)
    load = engine.start()
    async with asyncio.timeout(1):
        await started.wait()
        load.cancel()
        with pytest.raises(asyncio.CancelledError):
            await load
    assert cleaned.is_set()
    await engine.stop()


@pytest.mark.asyncio
async def test_failure_limit_cancels_other_handlers():
    started = asyncio.Event()
    cleaned = asyncio.Event()

    async def handler(value: int) -> int:
        if value == 0:
            await started.wait()
            raise ValueError("failed task")
        started.set()
        try:
            await asyncio.Event().wait()
            return value
        finally:
            cleaned.set()

    engine = Aqute(handler, 2, total_failed_tasks_limit=1)
    await engine.add_task(0)
    await engine.add_task(1)
    async with asyncio.timeout(1):
        with pytest.raises(AquteTooManyTasksFailedError):
            await engine.start_and_wait()
    assert cleaned.is_set()
    with pytest.raises(AquteTooManyTasksFailedError):
        await engine.stop()


@pytest.mark.asyncio
async def test_failure_limit_unblocks_bounded_producer():
    async def handler(_value: int) -> int:
        raise ValueError("failed task")

    engine = Aqute(handler, 1, input_task_queue_size=1, total_failed_tasks_limit=1)
    async with asyncio.timeout(1):
        with pytest.raises(AquteTooManyTasksFailedError):
            async with engine:
                for value in range(100):
                    await engine.add_task(value)
                await engine.wait_till_end()


@pytest.mark.asyncio
async def test_stop_resets_failure_count_and_preserves_results():
    async def handler(_value: int) -> int:
        raise ValueError("failed task")

    engine = Aqute(handler, 1, total_failed_tasks_limit=2)
    for value in range(2):
        async with engine:
            await engine.add_task(value)
            await engine.wait_till_end()
    results = engine.extract_all_results()
    assert [task.data for task in results] == [0, 1]
    assert all(isinstance(task.error, ValueError) for task in results)


@pytest.mark.asyncio
async def test_rate_limiter_failure_reaches_caller():
    class FailingLimiter:
        async def acquire(self, name="", task=None):  # noqa: ARG002
            raise RuntimeError("limiter unavailable")

    engine = Aqute(echo, 1, rate_limiter=FailingLimiter())
    async with asyncio.timeout(1):
        with pytest.raises(ExceptionGroup) as caught:
            await engine.apply_to_all([1])
    assert len(caught.value.exceptions) == 1
    assert isinstance(caught.value.exceptions[0], RuntimeError)
    assert str(caught.value.exceptions[0]) == "limiter unavailable"


@pytest.mark.asyncio
async def test_rate_limiter_failure_unblocks_retry_on_full_queue():
    class FailingOnSecondAcquire:
        def __init__(self):
            self.calls = 0

        async def acquire(self, name="", task=None):  # noqa: ARG002
            self.calls += 1
            if self.calls == 2:
                raise RuntimeError("limiter unavailable")

    async def handler(_value: int) -> int:
        raise ValueError("retry this task")

    async def run():
        async with Aqute(
            handler,
            1,
            retry_count=1,
            input_task_queue_size=1,
            rate_limiter=FailingOnSecondAcquire(),
        ) as engine:
            for value in range(4):
                await engine.add_task(value)
            await engine.wait_till_end()

    operation = asyncio.create_task(run())
    try:
        # Cancellation cleanup can itself raise the limiter error. Check completion
        # before cancelling so that cleanup cannot hide a stalled retry.
        done, _ = await asyncio.wait((operation,), timeout=1)
        assert operation in done
        with pytest.raises(ExceptionGroup) as caught:
            await operation
        assert len(caught.value.exceptions) == 1
        assert str(caught.value.exceptions[0]) == "limiter unavailable"
    finally:
        operation.cancel()
        with contextlib.suppress(asyncio.CancelledError, ExceptionGroup):
            await operation


@pytest.mark.asyncio
async def test_operations_after_load_cancellation_raise_aqute_error():
    engine = Aqute(echo, 1)
    load = engine.start()
    load.cancel()
    with pytest.raises(asyncio.CancelledError):
        await load
    with pytest.raises(AquteError, match="cancelled"):
        await engine.add_task(1)
    with pytest.raises(AquteError, match="cancelled"):
        await engine.get_task_result()
    await engine.stop()


@pytest.mark.asyncio
async def test_stop_unblocks_producer_with_aqute_error():
    started = asyncio.Event()

    async def handler(value: int) -> int:
        started.set()
        await asyncio.Event().wait()
        return value

    engine = Aqute(handler, 1, input_task_queue_size=1)
    engine.start()
    await engine.add_task(0)
    await started.wait()
    await engine.add_task(1)
    producer = asyncio.create_task(engine.add_task(2))
    try:
        await asyncio.sleep(0)
        async with asyncio.timeout(1):
            await engine.stop()
            with pytest.raises(AquteError, match="cancelled"):
                await producer
    finally:
        producer.cancel()
        with contextlib.suppress(asyncio.CancelledError, AquteError):
            await producer
        await engine.stop()


@pytest.mark.asyncio
async def test_stop_rejects_tasks_while_handler_cleanup_is_pending():
    started = asyncio.Event()
    cleanup_started = asyncio.Event()
    finish_cleanup = asyncio.Event()

    async def handler(value: int) -> int:
        started.set()
        try:
            await asyncio.Event().wait()
            return value
        finally:
            cleanup_started.set()
            await finish_cleanup.wait()

    engine = Aqute(handler, 1)
    engine.start()
    await engine.add_task(0)
    await started.wait()
    stopping = asyncio.create_task(engine.stop())
    try:
        async with asyncio.timeout(1):
            await cleanup_started.wait()
            with pytest.raises(AquteError, match="cancelled"):
                await engine.add_task(1)
    finally:
        finish_cleanup.set()
        await stopping


@pytest.mark.asyncio
@pytest.mark.parametrize("task_timeout", [0, -1])
async def test_nonpositive_timeout_does_not_call_handler(task_timeout):
    calls = []

    async def handler(value: int) -> int:
        calls.append(value)
        return value

    results = await Aqute(handler, 1, task_timeout_seconds=task_timeout).apply_to_all(
        [1]
    )
    assert calls == []
    assert isinstance(results[0].error, AquteTaskTimeoutError)


@pytest.mark.parametrize("workers_count", [0, -1])
def test_reject_nonpositive_worker_count(workers_count):
    with pytest.raises(ValueError, match="workers_count"):
        Aqute(echo, workers_count)
