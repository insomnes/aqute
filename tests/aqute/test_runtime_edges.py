import asyncio
import contextlib

import pytest

from aqute import Aqute, AquteError


@pytest.mark.asyncio
@pytest.mark.parametrize("consumer", ["finish", "iterator"])
@pytest.mark.parametrize("prior_cancellation", [False, True])
async def test_independent_stop_fails_consumer_after_cleanup(
    consumer, prior_cancellation
):
    """An independent stop raises AquteError, even after a caught cancellation."""
    tasks_before = asyncio.all_tasks()
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

    async def consume():
        if prior_cancellation:
            caller = asyncio.current_task()
            assert caller is not None
            caller.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.sleep(0)
        if consumer == "finish":
            await engine.add_task(1)
            engine.start()
            await engine.finish()
        else:
            async with engine.iter_results([1]) as results:
                await anext(results)

    operation = asyncio.create_task(consume())
    try:
        async with asyncio.timeout(1):
            await started.wait()
            await engine.stop()
        done, _ = await asyncio.wait((operation,), timeout=1)
        assert operation in done
        with pytest.raises(AquteError, match="cancelled"):
            await operation
        assert cleaned.is_set()
        assert asyncio.all_tasks() == tasks_before
    finally:
        operation.cancel()
        with contextlib.suppress(asyncio.CancelledError, AquteError):
            await operation
        await engine.stop()


@pytest.mark.asyncio
async def test_iterator_observes_cancelled_load_after_last_result():
    """Stopping after the last yield must raise AquteError on iterator completion."""

    async def handler(value: int) -> int:
        return value

    engine = Aqute(handler, 1)
    async with engine.iter_results([1]) as results:
        assert (await anext(results)).unwrap() == 1
        await engine.stop()
        with pytest.raises(AquteError, match="cancelled"):
            await anext(results)


@pytest.mark.asyncio
@pytest.mark.parametrize("consumer", ["finish", "iterator"])
@pytest.mark.parametrize("cancellation", ["cancel", "deadline"])
async def test_consumer_cancellation_propagates_after_cleanup(consumer, cancellation):
    """Caller cancellation and deadlines propagate after owned handler cleanup."""
    tasks_before = asyncio.all_tasks()
    started = asyncio.Event()
    cleaning = asyncio.Event()
    release = asyncio.Event()
    cleaned = asyncio.Event()
    deadline = asyncio.timeout(None)

    async def handler(value: int) -> int:
        started.set()
        try:
            await asyncio.Event().wait()
            return value
        finally:
            cleaning.set()
            await release.wait()
            cleaned.set()

    engine = Aqute(handler, 1)

    async def consume():
        async with deadline:
            if consumer == "finish":
                await engine.add_task(1)
                engine.start()
                await engine.finish()
            else:
                async with engine.iter_results([1]) as results:
                    await anext(results)

    operation = asyncio.create_task(consume())
    try:
        async with asyncio.timeout(1):
            await started.wait()
            if cancellation == "cancel":
                operation.cancel()
            else:
                deadline.reschedule(asyncio.get_running_loop().time())
            await cleaning.wait()
            assert not operation.done()
            release.set()
        done, _ = await asyncio.wait((operation,), timeout=1)
        assert operation in done
        expected = asyncio.CancelledError if cancellation == "cancel" else TimeoutError
        with pytest.raises(expected):
            await operation
        assert cleaned.is_set()
        assert asyncio.all_tasks() == tasks_before
    finally:
        release.set()
        operation.cancel()
        with contextlib.suppress(asyncio.CancelledError, TimeoutError):
            await operation
        await engine.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("start_timeout", [0, -1])
async def test_prequeued_input_does_not_expire_startup(start_timeout):
    """Prequeued input reaches the handler despite a nonpositive startup timeout."""

    async def handler(value: int) -> int:
        await asyncio.sleep(0)
        return value + 1

    engine = Aqute(handler, 1, start_timeout_seconds=start_timeout)
    await engine.add_task(1)
    try:
        async with asyncio.timeout(1):
            await engine.run()
        assert [task.unwrap() for task in engine.drain_results()] == [2]
    finally:
        await engine.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("start_timeout", [0, -1])
async def test_empty_startup_still_expires(start_timeout):
    """A nonpositive startup timeout fails when no input or completion is ready."""

    async def handler(value: int) -> int:
        return value

    engine = Aqute(handler, 1, start_timeout_seconds=start_timeout)
    load = engine.start()
    try:
        done, _ = await asyncio.wait((load,), timeout=1)
        assert load in done
        with pytest.raises(AquteError, match="Waited too long"):
            await load
    finally:
        with contextlib.suppress(AquteError):
            await engine.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("start_timeout", [0, -1])
async def test_finished_empty_input_needs_no_startup_wait(start_timeout):
    """Completing intentionally empty input succeeds with a nonpositive timeout."""

    async def handler(value: int) -> int:
        return value

    async with Aqute(handler, 1, start_timeout_seconds=start_timeout) as engine:
        async with asyncio.timeout(1):
            await engine.finish()
        assert engine.drain_results() == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("selection", "exclusion", "expected_calls"),
    [((), None, 1), (None, None, 3), ((ValueError,), ValueError, 1)],
)
async def test_retry_selection_respects_empty_tuple_and_exclusions(
    selection, exclusion, expected_calls
):
    """Empty selection and exclusions prevent retries; None retains the budget."""
    calls = 0

    async def handler(_value: int) -> int:
        nonlocal calls
        calls += 1
        raise ValueError("handler failed")

    engine = Aqute(
        handler,
        1,
        retry_count=2,
        specific_errors_to_retry=selection,
        errors_to_not_retry=exclusion,
    )
    (result,) = await engine.process_all([1])
    assert calls == expected_calls
    with pytest.raises(ValueError, match="handler failed"):
        result.unwrap()
