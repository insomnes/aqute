import asyncio
import contextlib
from collections import Counter

import pytest

from aqute import Aqute


async def echo(value: int) -> int:
    return value


@pytest.mark.asyncio
@pytest.mark.parametrize("collect", [False, True])
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("values", [[], [1, 2]])
async def test_helpers_complete_with_bounded_queues(collect, asynchronous, values):
    async def source():
        for value in values:
            yield value

    items = source() if asynchronous else iter(values)
    engine = Aqute(
        echo,
        1,
        input_task_queue_size=1,
        result_queue=asyncio.Queue(1),
        start_timeout_seconds=0.2,
    )
    async with asyncio.timeout(1):
        if collect:
            results = await engine.process_all(items)
        else:
            results = [result async for result in engine.iter_results(items)]
    assert [result.result for result in results] == values
    assert all(result.success for result in results)


@pytest.mark.asyncio
async def test_first_result_precedes_async_source_exhaustion():
    release = asyncio.Event()

    async def source():
        yield 1
        await release.wait()
        yield 2

    engine = Aqute(echo, 1)
    async with contextlib.aclosing(engine.iter_results(source())) as results:
        async with asyncio.timeout(1):
            assert (await anext(results)).result == 1
            release.set()
            assert (await anext(results)).result == 2
            with pytest.raises(StopAsyncIteration):
                await anext(results)


@pytest.mark.asyncio
async def test_iterator_yields_in_completion_order():
    release = asyncio.Event()

    async def handler(value: int) -> int:
        if value == 1:
            await release.wait()
        return value

    engine = Aqute(handler, 2, input_task_queue_size=1)
    async with contextlib.aclosing(engine.iter_results([1, 2])) as results:
        async with asyncio.timeout(1):
            assert (await anext(results)).result == 2
            release.set()
            assert (await anext(results)).result == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("manual", [False, True])
async def test_slow_consumption_bounds_production_and_handlers(manual):
    # I + 2R + W + 3 includes a producer item and the currently yielded result.
    bound = 1 + 2 * 1 + 2 + 3
    produced = 0
    handled = 0
    overflow = asyncio.Event()
    source_closed = False

    def source():
        nonlocal produced, source_closed
        try:
            for value in range(100):
                produced += 1
                if produced > bound:
                    overflow.set()
                yield value
        finally:
            source_closed = True

    async def handler(value: int) -> int:
        nonlocal handled
        handled += 1
        return value

    engine = Aqute(handler, 2, input_task_queue_size=1, result_queue=asyncio.Queue(1))
    async with asyncio.timeout(2):
        if manual:
            async with engine:

                async def produce():
                    for value in source():
                        await engine.add_task(value)
                    engine.finish_submitting()

                producer = asyncio.create_task(produce())
                try:
                    first = await engine.get_result()
                    with pytest.raises(TimeoutError):
                        async with asyncio.timeout(0.05):
                            await overflow.wait()
                    assert 1 <= handled <= produced <= bound
                    results = [first]
                    for _ in range(99):
                        results.append(await engine.get_result())
                    await producer
                    await engine.finish()
                finally:
                    producer.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await producer
        else:
            async with contextlib.aclosing(engine.iter_results(source())) as stream:
                first = await anext(stream)
                with pytest.raises(TimeoutError):
                    async with asyncio.timeout(0.05):
                        await overflow.wait()
                assert 1 <= handled <= produced <= bound
                results = [first, *[item async for item in stream]]
    assert [
        result.result for result in sorted(results, key=lambda task: task.data)
    ] == list(range(100))
    assert handled == produced == 100
    assert source_closed


@pytest.mark.asyncio
@pytest.mark.parametrize("priority", [False, True])
async def test_retries_complete_with_slow_consumption_and_bounded_queues(priority):
    attempts = Counter()

    async def handler(value: int) -> int:
        attempts[value] += 1
        if attempts[value] < 3:
            raise ValueError("retry")
        return value

    engine = Aqute(
        handler,
        2,
        retry_count=2,
        input_task_queue_size=1,
        result_queue=asyncio.Queue(1),
        use_priority_queue=priority,
    )
    async with asyncio.timeout(2):
        results = []
        async for result in engine.iter_results(range(20)):
            results.append(result)
            await asyncio.sleep(0.001)
    assert all(result.success for result in results)
    assert [
        result.result for result in sorted(results, key=lambda task: task.data)
    ] == list(range(20))
    assert attempts == Counter({value: 3 for value in range(20)})


@pytest.mark.asyncio
async def test_source_failure_cancels_active_handler():
    started = asyncio.Event()
    cleaned = asyncio.Event()

    async def handler(value: int) -> int:
        started.set()
        try:
            await asyncio.Event().wait()
            return value
        finally:
            cleaned.set()

    async def source():
        yield 1
        await started.wait()
        raise ValueError("source failed")

    engine = Aqute(handler, 1, input_task_queue_size=1)
    async with asyncio.timeout(1):
        with pytest.raises(ValueError, match="source failed"):
            await engine.process_all(source())
    assert cleaned.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_iterator_close_cleans_source_and_handler(asynchronous):
    started = asyncio.Event()
    cleaned = asyncio.Event()
    source_closed = asyncio.Event()

    async def handler(value: int) -> int:
        if value:
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleaned.set()
        return value

    def source():
        try:
            yield from range(100)
        finally:
            source_closed.set()

    async def async_source():
        try:
            for value in range(100):
                yield value
        finally:
            source_closed.set()

    engine = Aqute(handler, 1, input_task_queue_size=1, result_queue=asyncio.Queue(1))
    items = async_source() if asynchronous else source()
    async with asyncio.timeout(1):
        async with contextlib.aclosing(engine.iter_results(items)) as results:
            assert (await anext(results)).result == 0
            await started.wait()
    assert cleaned.is_set()
    assert source_closed.is_set()
    assert (await engine.process_all([0]))[0].result == 0


@pytest.mark.asyncio
async def test_cancelling_collection_cleans_waiting_source():
    started = asyncio.Event()
    cleaned = asyncio.Event()

    async def source():
        try:
            started.set()
            await asyncio.Event().wait()
            yield 1
        finally:
            cleaned.set()

    engine = Aqute(echo, 1)
    operation = asyncio.create_task(engine.process_all(source()))
    async with asyncio.timeout(1):
        await started.wait()
        operation.cancel()
        with pytest.raises(asyncio.CancelledError):
            await operation
    assert cleaned.is_set()
    assert (await engine.process_all([1]))[0].success


@pytest.mark.asyncio
async def test_drain_retained_results_before_reusing_helper():
    completed = asyncio.Event()
    handled = []

    async def handler(value: int) -> int:
        handled.append(value)
        if len(handled) == 3:
            completed.set()
        return value

    engine = Aqute(handler, 2)
    async with asyncio.timeout(1):
        async with contextlib.aclosing(engine.iter_results([1, 2, 3])) as results:
            first = await anext(results)
            await completed.wait()
            # Waiting for the public run drains completed work into retained results.
            await engine.finish()
        retained = engine.drain_results()
        assert sorted([first.data, *[result.data for result in retained]]) == [1, 2, 3]
        fresh = await engine.process_all([4])
    assert [result.result for result in fresh] == [4]
