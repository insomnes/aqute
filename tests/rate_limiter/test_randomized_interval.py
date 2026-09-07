import asyncio
import random
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
    max_rate, time_period, upper_bound = 2, 0.1, 1.1
    limiter = RandomizedIntervalRateLimiter(
        max_rate, time_period, upper_multiplier_bound=upper_bound
    )
    start = perf_counter()
    await limiter.acquire()
    await limiter.acquire()
    await limiter.acquire()
    elapsed_time = perf_counter() - start
    top_cap = time_period + time_period / max_rate * upper_bound * 2
    assert check_value_in_interval(elapsed_time, 0.1, top_cap)


@pytest.mark.asyncio
async def test_simultaneous_rate_limiting():
    """
    Same but with multiple coroutines
    """
    max_rate, time_period, upper_bound = 2, 0.1, 1.1
    limiter = RandomizedIntervalRateLimiter(
        max_rate, time_period, upper_multiplier_bound=upper_bound
    )

    async def simulator(rl: RandomizedIntervalRateLimiter):
        await rl.acquire()

    start = perf_counter()
    await asyncio.gather(*[simulator(limiter) for _ in range(3)])
    elapsed_time = perf_counter() - start
    top_cap = time_period + time_period / max_rate * upper_bound * 2
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


@pytest.mark.asyncio
async def test_startup_has_randomized_delay():
    """A fresh limiter must not always grant immediately because its index is zero."""
    state = random.getstate()
    random.seed(1)
    try:
        limiter = RandomizedIntervalRateLimiter(1, 0.1)
        start = perf_counter()
        await limiter.acquire()
        assert perf_counter() - start >= 0.01
    finally:
        random.setstate(state)


@pytest.mark.asyncio
async def test_minimum_delay_with_available_quota_and_after_idle():
    """Available quota must not bypass the additional configured minimum delay."""
    max_rate, time_period, lower_bound = 2, 0.1, 0.3
    limiter = RandomizedIntervalRateLimiter(
        max_rate, time_period, lower_multiplier_bound=lower_bound
    )
    for pause in (0, 0, time_period * 1.1):
        await asyncio.sleep(pause)
        start = perf_counter()
        await limiter.acquire()
        assert perf_counter() - start >= time_period / max_rate * lower_bound
