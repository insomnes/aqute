import asyncio
import contextlib
from collections import Counter

import pytest

from aqute import Aqute, AquteCounters


async def echo(value: int) -> int:
    return value


@pytest.mark.asyncio
async def test_pending_excludes_blocked_submission_and_running_includes_handler():
    entered = asyncio.Event()
    release = asyncio.Event()

    async def handler(value: int) -> int:
        entered.set()
        await release.wait()
        return value

    engine = Aqute(handler, 1, input_task_queue_size=1, result_queue=asyncio.Queue(0))
    assert engine.counters == AquteCounters(0, 0, 0, 0, 0)
    async with engine:
        await engine.add_task(1)
        await entered.wait()
        await engine.add_task(2)
        submitting = asyncio.create_task(engine.add_task(3))
        try:
            await asyncio.sleep(0)
            snapshot = engine.counters
            assert snapshot == AquteCounters(1, 1, 0, 0, 0)
            assert not submitting.done()
            with pytest.raises(AttributeError):
                snapshot.running = 99  # ty: ignore[invalid-assignment]
            release.set()
            await submitting
            await engine.finish()
            assert engine.counters == AquteCounters(0, 0, 3, 0, 0)
            assert snapshot == AquteCounters(1, 1, 0, 0, 0)
            assert len(engine.drain_results()) == 3
            assert engine.counters.succeeded == 3
        finally:
            submitting.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await submitting
    assert engine.counters == AquteCounters(0, 0, 0, 0, 0)


@pytest.mark.asyncio
async def test_terminal_counts_survive_result_consumption_and_reset_on_reuse():
    attempts = Counter()

    async def handler(value: int) -> int:
        attempts[value] += 1
        if value == 0 or (value == 1 and attempts[value] == 1):
            raise ValueError("retry")
        return value

    engine = Aqute(handler, 2, retry_count=1, result_queue=asyncio.Queue(0))
    async with engine:
        for value in range(3):
            await engine.add_task(value)
        await engine.finish()
        snapshot = AquteCounters(0, 0, 2, 1, 2)
        assert engine.counters == snapshot
        first = await engine.get_result()
        assert first.data in range(3)
        assert engine.counters == snapshot
        # Retain the other two results across stop().
    assert engine.counters == AquteCounters(0, 0, 0, 0, 0)
    assert len(engine.drain_results()) == 2
    async with engine:
        await engine.add_task(3)
        await engine.finish()
        assert engine.counters == AquteCounters(0, 0, 1, 0, 0)
        assert (await engine.get_result()).result == 3


@pytest.mark.asyncio
async def test_retry_count_waits_for_handler_after_backoff_and_rate_limit():
    delayed = asyncio.Event()
    retry_rate_wait = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    class Limiter:
        def __init__(self):
            self.calls = 0

        async def acquire(self, name="", task=None):  # noqa: ARG002
            self.calls += 1
            if self.calls == 2:
                retry_rate_wait.set()
                await release.wait()

    async def handler(value: int) -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ValueError("retry")
        return value

    def delay(_attempt: int, _error: Exception) -> float:
        delayed.set()
        return 0.02

    engine = Aqute(handler, 1, retry_count=1, retry_delay=delay, rate_limiter=Limiter())
    async with asyncio.timeout(1):
        async with engine:
            await engine.add_task(7)
            await delayed.wait()
            assert engine.counters == AquteCounters(0, 1, 0, 0, 0)
            await retry_rate_wait.wait()
            assert engine.counters == AquteCounters(0, 1, 0, 0, 0)
            release.set()
            await engine.finish()
            assert engine.counters == AquteCounters(0, 0, 1, 0, 1)
            assert (await engine.get_result()).result == 7
    assert calls == 2


@pytest.mark.asyncio
async def test_cancellation_is_not_a_terminal_handler_outcome():
    started = asyncio.Event()
    cleaned = asyncio.Event()

    async def handler(value: int) -> int:
        started.set()
        try:
            await asyncio.Event().wait()
            return value
        finally:
            cleaned.set()

    engine = Aqute(handler, 1)
    await engine.add_task(1)
    load = engine.start()
    await started.wait()
    load.cancel()
    with pytest.raises(asyncio.CancelledError):
        await load
    assert cleaned.is_set()
    assert engine.counters == AquteCounters(0, 0, 0, 0, 0)
    await engine.stop()


@pytest.mark.asyncio
async def test_terminal_counts_include_results_waiting_for_consumption():
    engine = Aqute(echo, 1, input_task_queue_size=1, result_queue=asyncio.Queue(1))
    async with engine:
        for value in range(3):
            await engine.add_task(value)
        results = [await engine.get_result() for _ in range(2)]
        await engine.finish()
        assert engine.counters == AquteCounters(0, 0, 3, 0, 0)
        results.append(await engine.get_result())
        assert [result.result for result in results] == [0, 1, 2]
        assert engine.counters == AquteCounters(0, 0, 3, 0, 0)
