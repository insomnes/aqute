import asyncio
from collections import Counter
from itertools import pairwise
from time import monotonic

import pytest

from aqute import Aqute
from aqute.ratelimiter import SlidingRateLimiter


@pytest.mark.asyncio
async def test_backoff_is_permitted_attempt_only_and_outside_timeout():
    calls = []
    selected = []
    failures = [ValueError("first"), ValueError("second")]

    async def handler(value: int) -> int:
        calls.append(monotonic())
        if len(calls) < 3:
            raise failures[len(calls) - 1]
        return value

    def delay(attempt: int, error: Exception) -> float:
        selected.append((attempt, error))
        return 0.03

    engine = Aqute(
        handler, 1, retry_count=2, retry_delay=delay, task_timeout_seconds=0.005
    )
    results = await engine.apply_to_all([7])
    assert results[0].result == 7 and results[0].success
    assert selected == [(1, failures[0]), (2, failures[1])]
    assert all(later - earlier >= 0.025 for earlier, later in pairwise(calls))


@pytest.mark.asyncio
@pytest.mark.parametrize("excluded", [False, True])
async def test_delay_is_not_called_for_exhausted_or_excluded_retries(excluded):
    calls = 0
    delays = []

    async def handler(value: int) -> int:
        nonlocal calls
        calls += 1
        raise ValueError(str(value))

    def delay(attempt: int, error: Exception) -> float:
        delays.append((attempt, str(error)))
        return 0

    engine = Aqute(
        handler,
        1,
        retry_count=1,
        retry_delay=delay,
        errors_to_not_retry=ValueError if excluded else None,
    )
    result = (await engine.apply_to_all([7]))[0]
    assert isinstance(result.error, ValueError)
    assert calls == (1 if excluded else 2)
    assert delays == ([] if excluded else [(1, "7")])


@pytest.mark.asyncio
@pytest.mark.parametrize("seconds", [-1, float("nan"), float("inf"), -float("inf")])
async def test_invalid_delay_propagates_as_worker_failure(seconds):
    calls = []

    async def handler(value: int) -> int:
        calls.append(value)
        raise ValueError("handler failure")

    def delay(_attempt: int, _error: Exception) -> float:
        return seconds

    engine = Aqute(handler, 1, retry_count=1, retry_delay=delay)
    with pytest.raises(ExceptionGroup) as caught:
        await engine.apply_to_all([1])
    assert len(caught.value.exceptions) == 1
    error = caught.value.exceptions[0]
    assert isinstance(error, ValueError)
    assert str(error) == "retry_delay must return finite, nonnegative seconds"
    assert calls == [1]


@pytest.mark.asyncio
async def test_backoff_does_not_block_other_available_worker():
    attempts = Counter()

    async def handler(value: int) -> int:
        attempts[value] += 1
        if value == 1 and attempts[value] == 1:
            raise ValueError("retry")
        return value

    def delay(_attempt: int, _error: Exception) -> float:
        return 0.05

    engine = Aqute(handler, 2, retry_count=1, retry_delay=delay)
    results = [result async for result in engine.apply_to_each([1, 2])]
    assert [result.result for result in results] == [2, 1]
    assert attempts == {1: 2, 2: 1}


@pytest.mark.asyncio
async def test_cancel_during_backoff_prevents_another_attempt():
    delaying = asyncio.Event()
    calls = []

    async def handler(value: int) -> int:
        calls.append(value)
        if value == 1:
            raise ValueError("retry")
        return value

    def delay(_attempt: int, _error: Exception) -> float:
        delaying.set()
        return 10

    engine = Aqute(handler, 1, retry_count=2, retry_delay=delay)
    operation = asyncio.create_task(engine.apply_to_all([1]))
    async with asyncio.timeout(1):
        await delaying.wait()
        operation.cancel()
        with pytest.raises(asyncio.CancelledError):
            await operation
        assert calls == [1]
        assert (await engine.apply_to_all([2]))[0].result == 2
    assert calls == [1, 2]


@pytest.mark.asyncio
async def test_delayed_retries_obey_rate_limit_and_bounded_streaming():
    calls = []
    attempts = Counter()

    async def handler(value: int) -> int:
        calls.append(monotonic())
        attempts[value] += 1
        if attempts[value] == 1:
            raise ValueError("retry")
        return value

    def delay(_attempt: int, _error: Exception) -> float:
        return 0.001

    engine = Aqute(
        handler,
        2,
        retry_count=1,
        retry_delay=delay,
        rate_limiter=SlidingRateLimiter(1, 0.02),
        input_task_queue_size=1,
        result_queue=asyncio.Queue(1),
    )
    async with asyncio.timeout(2):
        results = []
        async for result in engine.apply_to_each(range(4)):
            results.append(result)
            await asyncio.sleep(0.025)
    assert all(result.success for result in results)
    assert {result.result for result in results} == set(range(4))
    assert attempts == Counter({value: 2 for value in range(4)})
    assert all(later - earlier >= 0.018 for earlier, later in pairwise(calls))
