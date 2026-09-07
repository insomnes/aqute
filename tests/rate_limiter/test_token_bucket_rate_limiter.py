import asyncio
from time import perf_counter

import pytest

from aqute.ratelimiter import TokenBucketRateLimiter

from .checkers import check_value_in_interval


async def check_acquire_time(
    limiter: TokenBucketRateLimiter, lower: float, upper: float
):
    start = perf_counter()
    await limiter.acquire()
    elapsed_time = perf_counter() - start
    assert check_value_in_interval(elapsed_time, lower, upper)


@pytest.mark.asyncio
async def test_immediate_acquire():
    """
    We can acquire almost immediately at free limiter.
    """
    limiter = TokenBucketRateLimiter(10, 1)

    start = perf_counter()
    await limiter.acquire()
    elapsed_time = perf_counter() - start

    assert check_value_in_interval(elapsed_time, 0, 0.1)


@pytest.mark.asyncio
async def test_basic_ratelimiting():
    """
    We should wait at least time_period / max_rate before next request.
    """
    limiter = TokenBucketRateLimiter(2, 0.2)

    await limiter.acquire()

    await check_acquire_time(limiter, 0.1, 0.15)
    await check_acquire_time(limiter, 0.1, 0.15)


@pytest.mark.asyncio
async def test_bursting():
    """
    If bursting is awaliable, we should be able to acquire fast until capacity,
    if bucket is full (at start or after enough waiting time).
    """
    limiter = TokenBucketRateLimiter(2, 0.2, True)

    async def check_burst():
        # We can burst at full bucket
        await check_acquire_time(limiter, 0, 0.1)
        await check_acquire_time(limiter, 0, 0.1)

        # Next after burst should be at least 0.1 pause (our rate is req / 0.1 sec)
        await check_acquire_time(limiter, 0.1, 0.15)
        await check_acquire_time(limiter, 0.1, 0.15)

    await check_burst()
    # Filling bucket
    await asyncio.sleep(0.2)
    await check_burst()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("allow_burst", "acquired_before_idle"),
    [
        pytest.param(False, 0, id="delayed-first-use-no-burst"),
        pytest.param(True, 0, id="delayed-first-use-burst"),
        pytest.param(True, 1, id="partially-full-before-idle"),
    ],
)
async def test_idle_does_not_increase_capacity(allow_burst, acquired_before_idle):
    """An idle bucket grants its capacity, then waits for the next token."""
    max_rate = 4
    time_period = 0.2
    capacity = max_rate if allow_burst else 1
    limiter = TokenBucketRateLimiter(max_rate, time_period, allow_burst)

    for _ in range(acquired_before_idle):
        await limiter.acquire()
    await asyncio.sleep(time_period + 0.05)

    granted_at = []
    start = perf_counter()
    for _ in range(capacity + 1):
        await limiter.acquire()
        granted_at.append(perf_counter())

    assert granted_at[capacity - 1] - start < time_period / max_rate
    assert granted_at[capacity] - granted_at[0] >= time_period / max_rate - 0.001


@pytest.mark.asyncio
async def test_simultaneous_acquisitions():
    """
    Eleventh request should wait at least 0.4 seconds if we have rate for 5 requests
    per second (2 x max_rate + 1 => 2 x time_period).
    """
    limiter = TokenBucketRateLimiter(5, 0.2)

    async def acquire_token():
        await limiter.acquire()

    start = perf_counter()
    await asyncio.gather(*(acquire_token() for _ in range(11)))
    elapsed_time = perf_counter() - start

    assert check_value_in_interval(elapsed_time, 0.4, 0.45)


@pytest.mark.asyncio
async def test_simultaneous_steady_rate():
    """
    With token bucket algorithm we should be able to acquire on steady rate.
    """
    limiter = TokenBucketRateLimiter(10, 1)

    timings = []

    async def acquire_token():
        await limiter.acquire()
        timings.append(perf_counter())

    await asyncio.gather(*(acquire_token() for _ in range(11)))

    start = timings[0]
    elapsed = []
    for t in timings[1:]:
        elapsed_time = t - start
        # 1 / 10
        elapsed.append(check_value_in_interval(elapsed_time, 0.1, 0.15))
        start = t

    assert all(elapsed)
