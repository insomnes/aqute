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
