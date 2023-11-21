import asyncio
from time import perf_counter

import pytest

from aqute import Aqute
from aqute.ratelimiter import (
    PerWorkerRateLimiter,
    RandomizedIntervalRateLimiter,
    SlidingRateLimiter,
    TokenBucketRateLimiter,
)


async def non_failing_handler(i: int) -> str:
    await asyncio.sleep(0.01)
    return f"Success {i}"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rl_name",
    [
        "Sliding",
        "Token Bucket",
        "Randomized",
        "Per Worker",
    ],
)
async def test_with_ratelimiter(rl_name: str):
    # We should initialize them inside running loop
    limiters = {
        "Sliding": SlidingRateLimiter(5, 0.2),
        "Token Bucket": TokenBucketRateLimiter(5, 0.2),
        # This parameters should ensure passing of this tests
        "Randomized": RandomizedIntervalRateLimiter(
            5, 0.2, mean_target_multiplier=0.3, upper_multiplier_bound=0.5
        ),
        # We have 11 tasks for 10 workers, so per worker rate limiting should allow it
        # at rate of 1 request per 0.4 seconds (for this test to pass)
        "Per Worker": PerWorkerRateLimiter(1, 0.4),
    }
    rate_limiter = limiters[rl_name]
    aqute = Aqute(
        workers_count=10, handle_coro=non_failing_handler, rate_limiter=rate_limiter
    )

    start = perf_counter()
    await aqute.apply_to_all(range(11))
    elapsed_time = perf_counter() - start

    assert 0.4 < elapsed_time < 0.5
