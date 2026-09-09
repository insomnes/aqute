import asyncio
import contextlib

import pytest

from aqute import Aqute, AquteError


@pytest.mark.asyncio
@pytest.mark.parametrize("workers_count", [1, 2])
@pytest.mark.parametrize("task_timeout_seconds", [None, 10])
async def test_cancelled_handler_operation_terminates_process_all(
    workers_count, task_timeout_seconds
):
    """A cancelled dependency fails the run without retries and cleans siblings."""
    tasks_before = asyncio.all_tasks()
    sibling_started = asyncio.Event()
    calls = []
    cleaned = []

    async def handler(value: int) -> int:
        calls.append(value)
        try:
            if value == 1:
                if workers_count > 1:
                    await sibling_started.wait()
                dependency = asyncio.get_running_loop().create_future()
                dependency.cancel()
                await dependency
            else:
                sibling_started.set()
                await asyncio.Event().wait()
            return value
        finally:
            await asyncio.sleep(0)
            cleaned.append(value)

    engine = Aqute(
        handler,
        workers_count,
        retry_count=2,
        task_timeout_seconds=task_timeout_seconds,
    )
    operation = asyncio.create_task(engine.process_all([1, 2]))
    try:
        # Cancelling a stalled operation can expose errors only during cleanup.
        # Establish completion before cancellation can hide the original hang.
        done, _ = await asyncio.wait((operation,), timeout=1)
        assert operation in done
        with pytest.raises(ExceptionGroup) as caught:
            await operation
        assert len(caught.value.exceptions) == 1
        error = caught.value.exceptions[0]
        assert isinstance(error, AquteError)
        assert "cancelled unexpectedly" in str(error)
        assert isinstance(error.__cause__, asyncio.CancelledError)
        assert calls.count(1) == 1
        assert sorted(cleaned) == sorted(calls)
        if workers_count > 1:
            assert 2 in cleaned
        assert engine.drain_results() == []
        assert asyncio.all_tasks() == tasks_before
    finally:
        operation.cancel()
        with contextlib.suppress(asyncio.CancelledError, ExceptionGroup):
            await operation
        await engine.stop()


@pytest.mark.asyncio
async def test_cancelled_handler_operation_fails_manual_reader_and_finish():
    """Both manual completion boundaries fail after owned handler cleanup."""
    tasks_before = asyncio.all_tasks()
    sibling_started = asyncio.Event()
    release = asyncio.Event()
    cleaned = []

    async def handler(value: int) -> int:
        try:
            if value == 1:
                await sibling_started.wait()
                await release.wait()
                dependency = asyncio.get_running_loop().create_future()
                dependency.cancel()
                await dependency
            else:
                sibling_started.set()
                await asyncio.Event().wait()
            return value
        finally:
            await asyncio.sleep(0)
            cleaned.append(value)

    engine = Aqute(handler, 2)
    load = engine.start()
    await engine.add_task(1)
    await engine.add_task(2)
    reader = asyncio.create_task(engine.get_result())
    finishing = asyncio.create_task(engine.finish())
    try:
        release.set()
        done, _ = await asyncio.wait((reader, finishing, load), timeout=1)
        assert done == {reader, finishing, load}
        for operation in (reader, finishing, load):
            with pytest.raises(ExceptionGroup) as caught:
                await operation
            assert len(caught.value.exceptions) == 1
            assert isinstance(caught.value.exceptions[0], AquteError)
            assert "cancelled unexpectedly" in str(caught.value.exceptions[0])
        assert sorted(cleaned) == [1, 2]
        assert engine.drain_results() == []
        assert asyncio.all_tasks() == tasks_before
    finally:
        for operation in (reader, finishing, load):
            operation.cancel()
            with contextlib.suppress(asyncio.CancelledError, ExceptionGroup):
                await operation
        with contextlib.suppress(ExceptionGroup):
            await engine.stop()
