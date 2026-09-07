import asyncio
import contextlib
from collections import Counter
from io import StringIO

import pytest

from aqute import Aqute


async def echo(value: int) -> int:
    return value


@pytest.mark.asyncio
@pytest.mark.parametrize("collect", [False, True])
@pytest.mark.parametrize("batch_size", [0, -1, 1.5])
async def test_invalid_submission_batch_does_not_consume_input(collect, batch_size):
    """Invalid batching must fail without consuming input or preventing reuse."""
    consumed = []

    def source():
        consumed.append(1)
        yield 1

    engine = Aqute(echo, 1)
    with pytest.raises(ValueError, match="submission_batch_size"):
        if collect:
            await engine.process_all(source(), submission_batch_size=batch_size)
        else:
            async with engine.iter_results(
                source(), submission_batch_size=batch_size
            ) as results:
                await anext(results)
    assert consumed == []
    assert (await engine.process_all([2]))[0].result == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("collect", [False, True])
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("batch_size", [None, 4])
async def test_submission_batch_gives_ready_coroutines_a_turn(
    collect, asynchronous, batch_size
):
    """Even unbounded submission must give ready coroutines a turn after a batch."""
    produced = 0
    observer = None

    async def observe():
        return produced

    def source():
        nonlocal produced, observer
        for value in range(9):
            produced += 1
            if observer is None:
                observer = asyncio.create_task(observe())
            yield value

    async def async_source():
        for value in source():
            yield value

    items = async_source() if asynchronous else source()
    options = {} if batch_size is None else {"submission_batch_size": batch_size}
    engine = Aqute(echo, 2)
    async with asyncio.timeout(1):
        if collect:
            results = await engine.process_all(items, **options)
        else:
            async with engine.iter_results(items, **options) as stream:
                results = [result async for result in stream]
        assert observer is not None
        assert await observer == (1 if batch_size is None else batch_size)
    assert [
        result.result for result in sorted(results, key=lambda task: task.data)
    ] == list(range(9))


@pytest.mark.asyncio
@pytest.mark.parametrize("collect", [False, True])
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("values", [[], [1, 2]])
@pytest.mark.parametrize("batch_size", [1, 4, 32])
async def test_helpers_complete_with_bounded_queues(
    collect, asynchronous, values, batch_size
):
    """Full consumption returns each result and finishes generator source cleanup."""
    closed = []

    def source():
        try:
            yield from values
        finally:
            closed.append(True)

    async def async_source():
        try:
            for value in values:
                yield value
        finally:
            await asyncio.sleep(0)
            closed.append(True)

    items = async_source() if asynchronous else source()
    engine = Aqute(
        echo,
        1,
        input_task_queue_size=1,
        result_queue=asyncio.Queue(1),
        start_timeout_seconds=0.2,
    )
    async with asyncio.timeout(1):
        if collect:
            results = await engine.process_all(items, submission_batch_size=batch_size)
        else:
            async with engine.iter_results(
                items, submission_batch_size=batch_size
            ) as stream:
                results = [result async for result in stream]
    assert [result.result for result in results] == values
    assert all(result.success for result in results)
    assert closed == [True]


@pytest.mark.asyncio
@pytest.mark.parametrize("batch_size", [1, 32])
async def test_first_result_precedes_async_source_exhaustion(batch_size):
    release = asyncio.Event()

    async def source():
        yield 1
        await release.wait()
        yield 2

    engine = Aqute(echo, 1)
    async with engine.iter_results(
        source(), submission_batch_size=batch_size
    ) as results:
        async with asyncio.timeout(1):
            assert (await anext(results)).result == 1
            release.set()
            assert (await anext(results)).result == 2
            with pytest.raises(StopAsyncIteration):
                await anext(results)


@pytest.mark.asyncio
@pytest.mark.parametrize("batch_size", [1, 32])
async def test_iterator_yields_in_completion_order(batch_size):
    release = asyncio.Event()

    async def handler(value: int) -> int:
        if value == 1:
            await release.wait()
        return value

    engine = Aqute(handler, 2, input_task_queue_size=1)
    async with engine.iter_results([1, 2], submission_batch_size=batch_size) as results:
        async with asyncio.timeout(1):
            assert (await anext(results)).result == 2
            release.set()
            assert (await anext(results)).result == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(("manual", "batch_size"), [(True, 1), (False, 1), (False, 32)])
async def test_slow_consumption_bounds_production_and_handlers(manual, batch_size):
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
            async with engine.iter_results(
                source(), submission_batch_size=batch_size
            ) as stream:
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
@pytest.mark.parametrize("batch_size", [1, 32])
async def test_retries_complete_with_slow_consumption_and_bounded_queues(
    priority, batch_size
):
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
        async with engine.iter_results(
            range(20), submission_batch_size=batch_size
        ) as stream:
            async for result in stream:
                results.append(result)
                await asyncio.sleep(0.001)
    assert all(result.success for result in results)
    assert [
        result.result for result in sorted(results, key=lambda task: task.data)
    ] == list(range(20))
    assert attempts == Counter({value: 3 for value in range(20)})


@pytest.mark.asyncio
@pytest.mark.parametrize("batch_size", [1, 32])
@pytest.mark.parametrize("collect", [False, True])
async def test_source_failure_cancels_active_handler(batch_size, collect):
    """A source error reaches the caller after active handler cleanup completes."""
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

    async def source():
        yield 1
        await started.wait()
        raise ValueError("source failed")

    engine = Aqute(handler, 1, input_task_queue_size=1)
    async with asyncio.timeout(1):
        with pytest.raises(ValueError, match="source failed"):
            if collect:
                await engine.process_all(source(), submission_batch_size=batch_size)
            else:
                async with engine.iter_results(
                    source(), submission_batch_size=batch_size
                ) as results:
                    await anext(results)
    assert cleaned.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("batch_size", [1, 32])
@pytest.mark.parametrize("consumer_error", [False, True])
async def test_context_exit_cleans_source_and_handler(
    asynchronous, batch_size, consumer_error
):
    """Break or consumer failure must await cleanup and allow engine reuse."""
    started = asyncio.Event()
    cleaned = asyncio.Event()
    source_closed = asyncio.Event()

    async def handler(value: int) -> int:
        if value:
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                await asyncio.sleep(0)
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
            await asyncio.sleep(0)
            source_closed.set()

    engine = Aqute(handler, 1, input_task_queue_size=1, result_queue=asyncio.Queue(1))
    items = async_source() if asynchronous else source()

    async def consume():
        async with engine.iter_results(
            items, submission_batch_size=batch_size
        ) as results:
            async for result in results:
                assert result.result == 0
                await started.wait()
                if consumer_error:
                    raise ValueError("consumer failed")
                break

    async with asyncio.timeout(1):
        operation = asyncio.create_task(consume())
        error_context = (
            pytest.raises(ValueError, match="consumer failed")
            if consumer_error
            else contextlib.nullcontext()
        )
        with error_context:
            await operation
    assert cleaned.is_set()
    assert source_closed.is_set()
    assert (await engine.process_all([0]))[0].result == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("collect", "consume_result"), [(True, False), (False, False), (False, True)]
)
async def test_cancelling_helper_cleans_waiting_source(collect, consume_result):
    """Cancellation during iteration or consumption awaits source/handler cleanup."""
    started = asyncio.Event()
    cleaned = asyncio.Event()
    handled = asyncio.Event()
    handler_cleaned = asyncio.Event()
    consuming = asyncio.Event()

    async def handler(value: int) -> int:
        if value:
            handled.set()
            try:
                await asyncio.Event().wait()
            finally:
                await asyncio.sleep(0)
                handler_cleaned.set()
        return value

    async def source():
        try:
            yield 0
            yield 1
            started.set()
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0)
            cleaned.set()

    engine = Aqute(handler, 1)

    async def consume():
        if collect:
            await engine.process_all(source())
        else:
            async with engine.iter_results(source()) as results:
                assert (await anext(results)).result == 0
                consuming.set()
                if consume_result:
                    await asyncio.Event().wait()
                else:
                    await anext(results)

    operation = asyncio.create_task(consume())
    async with asyncio.timeout(1):
        await started.wait()
        await handled.wait()
        if not collect:
            await consuming.wait()
        operation.cancel()
        with pytest.raises(asyncio.CancelledError):
            await operation
    assert cleaned.is_set()
    assert handler_cleaned.is_set()
    assert (await engine.process_all([0]))[0].success


@pytest.mark.asyncio
@pytest.mark.parametrize("batch_size", [1, 32])
async def test_drain_retained_results_before_reusing_helper(batch_size):
    completed = asyncio.Event()
    handled = []

    async def handler(value: int) -> int:
        handled.append(value)
        if len(handled) == 3:
            completed.set()
        return value

    engine = Aqute(handler, 2)
    async with asyncio.timeout(1):
        async with engine.iter_results(
            [1, 2, 3], submission_batch_size=batch_size
        ) as results:
            first = await anext(results)
            await completed.wait()
            # Waiting for the public run drains completed work into retained results.
            await engine.finish()
        retained = engine.drain_results()
        assert sorted([first.data, *[result.data for result in retained]]) == [1, 2, 3]
        fresh = await engine.process_all([4])
    assert [result.result for result in fresh] == [4]


@pytest.mark.asyncio
async def test_context_without_iteration_does_not_acquire_input():
    """An unused context leaves input untouched and its closed iterator cannot start."""
    acquired = []

    class Source:
        def __iter__(self):
            acquired.append(True)
            return iter([1])

    engine = Aqute(echo, 1)
    source = Source()
    async with engine.iter_results(source) as results:
        assert acquired == []
    with pytest.raises(StopAsyncIteration):
        await anext(results)
    assert acquired == []
    assert (await engine.process_all(source))[0].result == 1
    assert acquired == [True]


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_context_leaves_custom_source_resource_open(asynchronous):
    """Closing a partial stream must leave non-generator source resources usable."""
    resource = StringIO("1\n" * 100)

    class AsyncLines:
        def __aiter__(self):
            return self

        async def __anext__(self):
            line = resource.readline()
            if not line:
                raise StopAsyncIteration
            return line

        async def aclose(self):
            resource.close()

    async def parse(line: str) -> int:
        return int(line)

    engine = Aqute(parse, 1, input_task_queue_size=1, result_queue=asyncio.Queue(1))
    try:
        source = AsyncLines() if asynchronous else resource
        async with engine.iter_results(source) as results:
            assert (await anext(results)).result == 1
        assert not resource.closed
        resource.seek(0)
        assert resource.readline() == "1\n"
    finally:
        resource.close()
