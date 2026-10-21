import asyncio
import contextlib

import pytest

from aqute import Aqute


def exception_leaves(error: BaseException) -> list[BaseException]:
    if isinstance(error, BaseExceptionGroup):
        return [leaf for child in error.exceptions for leaf in exception_leaves(child)]
    return [error]


@pytest.mark.asyncio
@pytest.mark.parametrize("task_timeout_seconds", [None, 3600])
async def test_stop_surfaces_cleanup_failure_without_retry_or_queued_work(
    task_timeout_seconds,
):
    """Stop exposes cleanup errors without starting pending input, then resets."""
    entered = asyncio.Event()
    release = asyncio.Event()
    sentinel = RuntimeError("handler cleanup failed")
    calls = []

    async def handler(value: int) -> int:
        calls.append(value)
        if value == 1:
            entered.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                raise sentinel from None
        return value

    engine = Aqute(
        handler,
        1,
        input_task_queue_size=1,
        retry_count=1,
        specific_errors_to_retry=RuntimeError,
        task_timeout_seconds=task_timeout_seconds,
    )
    engine.start()
    await engine.add_task(1)
    await entered.wait()
    await engine.add_task(2)
    stopping = asyncio.create_task(engine.stop())
    try:
        # Observe completion before rescue cancellation can hide a shutdown hang.
        done, _ = await asyncio.wait((stopping,), timeout=1)
        assert stopping in done
        with pytest.raises(ExceptionGroup) as caught:
            await stopping
        leaves = exception_leaves(caught.value)
        assert len(leaves) == 1
        assert leaves[0] is sentinel
        assert calls == [1]
        assert engine.drain_results() == []
        assert tuple(engine.counters) == (0, 0, 0, 0, 0)
        await engine.stop()
        (result,) = await engine.process_all([3])
        assert result.unwrap() == 3
    finally:
        release.set()
        stopping.cancel()
        with contextlib.suppress(asyncio.CancelledError, ExceptionGroup):
            await stopping
        await engine.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("task_timeout_seconds", [None, 3600])
@pytest.mark.parametrize("retry_count", [0, 1])
async def test_handler_taskgroup_failure_remains_an_ordinary_task_error(
    task_timeout_seconds, retry_count
):
    """Handler-owned TaskGroup failure does not imply engine shutdown."""
    sentinel = ValueError("ordinary handler failure")
    calls = 0

    async def child() -> None:
        raise RuntimeError("child failed")

    async def handler(value: int) -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            try:
                async with asyncio.TaskGroup() as group:
                    group.create_task(child())
            except ExceptionGroup:
                raise sentinel from None
        return value

    engine = Aqute(
        handler,
        1,
        retry_count=retry_count,
        specific_errors_to_retry=ValueError,
        task_timeout_seconds=task_timeout_seconds,
    )
    (result,) = await engine.process_all([7])
    assert calls == retry_count + 1
    if retry_count:
        assert result.unwrap() == 7
    else:
        assert result.success is False
        assert result.error is sentinel


@pytest.mark.asyncio
@pytest.mark.parametrize("task_timeout_seconds", [None, 3600])
async def test_limiter_failure_surfaces_delayed_sibling_cleanup_error(
    task_timeout_seconds,
):
    """Fatal limiter errors cancel siblings without capturing their cleanup errors."""
    entered = asyncio.Event()
    cleaning = asyncio.Event()
    release = asyncio.Event()
    limiter_error = LookupError("limiter failed")
    cleanup_error = RuntimeError("sibling cleanup failed")
    calls = []

    class FailingLimiter:
        async def acquire(self, name="", task=None):  # noqa: ARG002
            if task is not None and task.data == 2:
                await entered.wait()
                raise limiter_error

    async def handler(value: int) -> int:
        calls.append(value)
        if value == 1:
            entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cleaning.set()
                await release.wait()
                raise cleanup_error from None
        return value

    engine = Aqute(
        handler,
        2,
        rate_limiter=FailingLimiter(),
        retry_count=1,
        specific_errors_to_retry=RuntimeError,
        task_timeout_seconds=task_timeout_seconds,
    )
    operation = asyncio.create_task(engine.process_all([1, 2]))
    try:
        async with asyncio.timeout(1):
            await cleaning.wait()
        done, _ = await asyncio.wait((operation,), timeout=0.05)
        assert not done
        release.set()
        done, _ = await asyncio.wait((operation,), timeout=1)
        assert operation in done
        with pytest.raises(ExceptionGroup) as caught:
            await operation
        leaves = exception_leaves(caught.value)
        assert len(leaves) == 2
        assert sum(leaf is limiter_error for leaf in leaves) == 1
        assert sum(leaf is cleanup_error for leaf in leaves) == 1
        assert calls == [1]
        assert engine.drain_results() == []
        (result,) = await engine.process_all([3])
        assert result.unwrap() == 3
    finally:
        release.set()
        operation.cancel()
        with contextlib.suppress(asyncio.CancelledError, ExceptionGroup):
            await operation
        await engine.stop()
