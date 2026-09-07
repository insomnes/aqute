import asyncio
from typing import Self

import pytest

from aqute import Aqute, AquteError
from aqute.ratelimiter import (
    PerWorkerRateLimiter,
    RandomizedIntervalRateLimiter,
    SlidingRateLimiter,
    TokenBucketRateLimiter,
)


@pytest.mark.parametrize(
    "limiter_type",
    [
        TokenBucketRateLimiter,
        SlidingRateLimiter,
        PerWorkerRateLimiter,
        RandomizedIntervalRateLimiter,
    ],
)
@pytest.mark.parametrize(
    ("max_rate", "time_period"), [(0, 1), (-1, 1), (1, 0), (1, -1)]
)
def test_rate_limiters_reject_nonpositive_configuration(
    limiter_type, max_rate, time_period
):
    """An invalid rate or period must raise ValueError during construction."""
    with pytest.raises(ValueError, match="Invalid values for configuration"):
        limiter_type(max_rate, time_period)


@pytest.mark.asyncio
async def test_submission_after_completion_requires_stop():
    """Late submission must fail; stop must allow a new run with fresh work."""

    async def handle(value: int) -> int:
        return value * 2

    engine = Aqute(handle, 1)
    async with engine:
        await engine.add_task(1)
        await engine.finish()
        assert (await engine.get_result()).result == 2
        with pytest.raises(AquteError, match="after load completion"):
            await engine.add_task(2)

    tasks = await engine.process_all([3])
    assert [task.result for task in tasks] == [6]
    assert all(task.success for task in tasks)


@pytest.mark.asyncio
async def test_custom_async_iterator_input():
    """Helpers must accept AsyncIterator implementations without generator methods."""

    class Input:
        def __init__(self) -> None:
            self.values = iter([1, 2, 3])

        def __aiter__(self) -> Self:
            return self

        async def __anext__(self) -> int:
            try:
                return next(self.values)
            except StopIteration:
                raise StopAsyncIteration from None

    async def handle(value: int) -> int:
        return value * 2

    engine = Aqute(handle, 2, input_task_queue_size=1, result_queue=asyncio.Queue(1))
    async with asyncio.timeout(1):
        tasks = await engine.process_all(Input())
    assert [task.result for task in tasks] == [2, 4, 6]
    assert all(task.success for task in tasks)
