import asyncio
from time import perf_counter

import pytest

from aqute.ratelimiter import RandomizedIntervalRateLimiter

from .checkers import check_value_in_interval


@pytest.mark.asyncio
async def test_basic_rate_limiting():
    """
    We should wait at least time period at next request after max rate, but no more,
    than time period + optimal sleep time * multiplier upper bound * 2
    """
    limiter = RandomizedIntervalRateLimiter(2, 0.1)
    start = perf_counter()
    await limiter.acquire()
    await limiter.acquire()
    await limiter.acquire()
    elapsed_time = perf_counter() - start
    top_cap = 0.1 + limiter._optimal_sleep * limiter._upper_bound * 2
    assert check_value_in_interval(elapsed_time, 0.1, top_cap)


@pytest.mark.asyncio
async def test_simultaneous_rate_limiting():
    """
    Same but with multiple coroutines
    """
    limiter = RandomizedIntervalRateLimiter(2, 0.1)

    async def simulator(rl: RandomizedIntervalRateLimiter):
        await rl.acquire()

    start = perf_counter()
    await asyncio.gather(*[simulator(limiter) for _ in range(3)])
    elapsed_time = perf_counter() - start
    top_cap = 0.1 + limiter._optimal_sleep * limiter._upper_bound * 2
    assert check_value_in_interval(elapsed_time, 0.1, top_cap)


@pytest.mark.asyncio
async def test_all_intervals():
    """
    Every max_rate request should be delayed by at least time period
    """
    max_rate = 5
    time_period = 0.1
    limiter = RandomizedIntervalRateLimiter(max_rate, time_period)

    timers = []

    async def simulator(rl: RandomizedIntervalRateLimiter):
        await rl.acquire()
        timers.append(perf_counter())

    await asyncio.gather(*[simulator(limiter) for _ in range(17)])
    failed = []
    for i in range(max_rate, len(timers)):
        delta = timers[i] - timers[i - max_rate]
        if delta < time_period:
            failed.append((i, i - max_rate, delta))

    assert not failed
