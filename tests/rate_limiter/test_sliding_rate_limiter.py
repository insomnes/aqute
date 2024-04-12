import asyncio
from time import perf_counter
from typing import Tuple

import pytest

from aqute.ratelimiter import SlidingRateLimiter

from .checkers import check_value_in_interval


@pytest.mark.asyncio
async def test_immediate_acquire():
    """
    We can acquire almost immediately at free limiter.
    """
    limiter = SlidingRateLimiter(10, 1)

    start = perf_counter()
    await limiter.acquire()
    elapsed_time = perf_counter() - start

    assert check_value_in_interval(elapsed_time, 0, 0.1)


@pytest.mark.asyncio
async def test_basic_rate_limiting():
    """
    We should wait at least 0.5 second for 6 on 5 / 0.5.
    """
    limiter = SlidingRateLimiter(5, 0.2)

    for _ in range(5):
        await limiter.acquire()

    start = perf_counter()
    await limiter.acquire()
    elapsed_time = perf_counter() - start

    assert check_value_in_interval(elapsed_time, 0.2, 0.25)


@pytest.mark.asyncio
async def test_multiple_periods():
    """
    We can acquire almost instant after waiting on full limiter longer than
    specified time period.
    """
    limiter = SlidingRateLimiter(5, 0.1)

    for _ in range(5):
        await limiter.acquire()

    await asyncio.sleep(0.2)

    start = perf_counter()
    for _ in range(5):
        await limiter.acquire()
    elapsed_time = perf_counter() - start

    assert check_value_in_interval(elapsed_time, 0, 0.1)


@pytest.mark.asyncio
async def test_simultaneous_immediate_acquire():
    """
    We can acquire almost immediately even with concurrently running coroutines.
    """
    limiter = SlidingRateLimiter(5, 1)

    async def acquire_token():
        await limiter.acquire()

    start = perf_counter()
    await asyncio.gather(*(acquire_token() for _ in range(5)))
    elapsed_time = perf_counter() - start

    assert check_value_in_interval(elapsed_time, 0, 0.1)


@pytest.mark.asyncio
async def test_boundary():
    """
    We are checking on concurrent example, that our basic logic works as expected:
      1. First five (as in rate limit config) acquire immediately
      2. Next five should wait at least 0.2 second (as in config) before acquire
      3. Finally the next one should also wait at least 0.2 second before acquire

    Thus we have our sliding window working
    """
    limiter = SlidingRateLimiter(5, 0.2)

    async def acquire_token():
        await limiter.acquire()

    async def run_five():
        await asyncio.gather(*(acquire_token() for _ in range(5)))

    start = perf_counter()
    await run_five()
    one_bound_elapsed = perf_counter() - start

    await run_five()
    two_bounds_elapsed = perf_counter() - start

    await limiter.acquire()
    elapsed_time = perf_counter() - start

    assert check_value_in_interval(one_bound_elapsed, 0, 0.1)
    assert check_value_in_interval(two_bounds_elapsed, 0.2, 0.25)
    assert check_value_in_interval(elapsed_time, 0.4, 0.45)


@pytest.mark.asyncio
async def test_simultaneous_acquisitions():
    """
    We are testing the same logic as in test_boundary here, but in more real life like
    looking scenario with all coroutines running via gather at once.
    """
    limiter = SlidingRateLimiter(5, 0.2)

    async def acquire_token():
        await limiter.acquire()

    start = perf_counter()
    await asyncio.gather(*(acquire_token() for _ in range(11)))
    elapsed_time = perf_counter() - start

    assert check_value_in_interval(elapsed_time, 0.4, 0.45)


@pytest.mark.asyncio
async def test_concurrent_delay_after_rate_exhaustion():
    """
    We are testing more or less the same here as in previous test, just to be deadly
    sure rate limiter works as expected on exhausted limiter.
    """

    limiter = SlidingRateLimiter(5, 0.2)
    for _ in range(5):
        await limiter.acquire()

    async def worker():
        await limiter.acquire()

    start = perf_counter()
    await asyncio.gather(*(worker() for _ in range(10)))
    elapsed_time = perf_counter() - start

    assert check_value_in_interval(elapsed_time, 0.4, 0.45)


@pytest.mark.asyncio
async def test_sliding_window_works_as_expected():
    """
    We are testing here the sliding window logic, that if we are not bursting all the
    coroutines immediately (simulated with different sleep time) they will acquire
    according to limit in order with each other.
    """
    limiter = SlidingRateLimiter(2, 1)

    async def worker(sleep_time: float) -> Tuple[float, float]:
        start = perf_counter()
        await asyncio.sleep(sleep_time)

        await limiter.acquire()
        end = perf_counter()
        return start, end

    result = await asyncio.gather(*[worker(st) for st in [0, 0.5, 0.6, 0.7]])

    first_start, first_end = result[0]
    second_start, second_end = result[1]
    _, third_end = result[2]
    _, fourth_end = result[3]

    # Not wait for first
    first_delta = first_end - first_start
    assert check_value_in_interval(first_delta, 0, 0.1)
    # No wait only sleep timer for second acquire
    second_delta = second_end - second_start
    assert check_value_in_interval(second_delta, 0.5, 0.6)
    # The diff between third acquire and first is ~1 second
    third_minus_first = third_end - first_end
    assert check_value_in_interval(third_minus_first, 1, 1.1)
    # Same for the fourth and second
    fourth_minus_second = fourth_end - second_end
    assert check_value_in_interval(fourth_minus_second, 1, 1.1)
    # The diff between fourth and third acquire should be ~ second sleep time
    fourth_minus_third = fourth_end - third_end
    assert check_value_in_interval(fourth_minus_third, 0.5, 0.6)
