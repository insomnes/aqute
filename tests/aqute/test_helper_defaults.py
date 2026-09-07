import asyncio
import contextlib
from collections import Counter

import pytest

from aqute import Aqute, AquteError


async def echo(value: int) -> int:
    return value


@pytest.mark.asyncio
@pytest.mark.parametrize("length", [100, 1000])
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("explicit", [False, True])
async def test_slow_consumer_limits_source_and_terminal_results(
    length, asynchronous, explicit
):
    """Paused consumption bounds progress; resuming returns every source item."""
    workers = 2
    bound = (1 + 2 * 1 if explicit else 3 * workers) + workers + 3
    produced = handled = 0
    overflow = asyncio.Event()

    def source():
        nonlocal produced
        for value in range(length):
            produced += 1
            if produced > bound:
                overflow.set()
            yield value

    async def async_source():
        for value in source():
            yield value

    async def handler(value: int) -> int:
        nonlocal handled
        handled += 1
        return value

    queue = asyncio.Queue(1) if explicit else None
    engine = (
        Aqute(handler, workers, input_task_queue_size=1, result_queue=queue)
        if explicit
        else Aqute(handler, workers)
    )
    items = async_source() if asynchronous else source()
    async with asyncio.timeout(2):
        async with engine.iter_results(items, submission_batch_size=8) as stream:
            first = await anext(stream)
            with pytest.raises(TimeoutError):
                async with asyncio.timeout(0.03):
                    await overflow.wait()
            assert 1 <= handled <= produced <= bound
            results = [first, *[task async for task in stream]]
    assert Counter(task.result for task in results) == Counter(range(length))
    assert handled == produced == length
    if queue is not None:
        assert engine.result_queue is queue
        assert queue.maxsize == 1
        async with asyncio.timeout(1):
            await engine.add_task(length)
            await engine.run()
            assert queue.get_nowait().result == length
            await engine.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("capacity", [None, 1, 0])
@pytest.mark.parametrize("collect", [False, True])
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_input_capacity_controls_admission_before_handlers_complete(
    capacity, collect, asynchronous
):
    """Finite input stops admission; explicit zero permits source exhaustion."""
    produced = started = 0
    workers_started = asyncio.Event()
    exhausted = asyncio.Event()
    release = asyncio.Event()

    def source():
        nonlocal produced
        for value in range(40):
            produced += 1
            yield value
        exhausted.set()

    async def async_source():
        for value in source():
            yield value

    async def handler(value: int) -> int:
        nonlocal started
        started += 1
        if started == 2:
            workers_started.set()
        await release.wait()
        return value

    engine = Aqute(handler, 2, input_task_queue_size=capacity)

    async def consume():
        items = async_source() if asynchronous else source()
        if collect:
            return await engine.process_all(items)
        async with engine.iter_results(items) as stream:
            return [task async for task in stream]

    operation = asyncio.create_task(consume())
    async with asyncio.timeout(2):
        try:
            await workers_started.wait()
            if capacity == 0:
                await exhausted.wait()
                assert produced == 40
            else:
                with pytest.raises(TimeoutError):
                    async with asyncio.timeout(0.03):
                        await exhausted.wait()
                assert 2 <= produced <= (2 if capacity is None else capacity) + 3
        finally:
            release.set()
            results = await operation
    assert Counter(task.result for task in results) == Counter(range(40))


@pytest.mark.asyncio
async def test_explicit_unlimited_result_queue_keeps_all_completed_results():
    """A supplied unlimited queue permits finishing before consuming the rest."""
    queue = asyncio.Queue(0)
    engine = Aqute(echo, 2, result_queue=queue)
    exhausted = asyncio.Event()

    def source():
        yield from range(40)
        exhausted.set()

    async with asyncio.timeout(2):
        async with engine.iter_results(source()) as stream:
            first = await anext(stream)
            await exhausted.wait()
            await engine.finish()
            assert engine.result_queue is queue
            assert queue.maxsize == 0
            assert queue.qsize() == 39
            results = [first, *[task async for task in stream]]
    assert Counter(task.result for task in results) == Counter(range(40))
    assert engine.result_queue is queue


@pytest.mark.asyncio
@pytest.mark.parametrize("exit_mode", ["complete", "break", "error", "cancel"])
async def test_helper_exit_retains_results_and_restores_manual_defaults(exit_mode):
    """Every exit preserves retained results and later manual preload/drain."""
    engine = Aqute(echo, 2)
    exhausted = asyncio.Event()

    def source():
        yield from range(3)
        exhausted.set()

    async def consume():
        async with engine.iter_results(source()) as stream:
            first = await anext(stream)
            await exhausted.wait()
            await engine.finish()
            if exit_mode == "complete":
                return [first, *[task async for task in stream]]
            if exit_mode == "error":
                raise ValueError("consumer failed")
            if exit_mode == "cancel":
                raise asyncio.CancelledError
            return [first]

    async with asyncio.timeout(2):
        if exit_mode in {"error", "cancel"}:
            error = ValueError if exit_mode == "error" else asyncio.CancelledError
            with pytest.raises(error):
                await consume()
            consumed = [0]
        else:
            consumed = [task.result for task in await consume()]
        retained = engine.drain_results()
        assert Counter([*consumed, *[task.result for task in retained]]) == Counter(
            range(3)
        )
        for value in range(40):
            await engine.add_task(value)
        await engine.run()
        assert Counter(task.result for task in engine.drain_results()) == Counter(
            range(40)
        )
        await engine.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["preloaded", "active", "retained"])
async def test_conflicting_helper_setup_does_not_consume_or_discard_work(state):
    """Conflicting helper setup fails before source acquisition and keeps work."""
    acquired = []

    class Source:
        def __iter__(self):
            acquired.append(True)
            return iter([99])

    engine = Aqute(echo, 2)
    await engine.add_task(7)
    if state == "active":
        engine.start()
    elif state == "retained":
        await engine.run()
        await engine.stop()
    try:
        with pytest.raises(AquteError):
            await engine.process_all(Source())
        assert acquired == []
        async with asyncio.timeout(1):
            if state != "retained":
                await engine.run()
            assert [task.result for task in engine.drain_results()] == [7]
    finally:
        with contextlib.suppress(asyncio.CancelledError):
            await engine.stop()
