import asyncio
from time import perf_counter

import pytest

from aqute import Aqute
from aqute.ratelimiter import (
    PerWorkerRateLimiter,
    SlidingRateLimiter,
    TokenBucketRateLimiter,
)


async def non_failing_handler(i: int) -> str:
    await asyncio.sleep(0.01)
    return f"Success {i}"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rl_name",
    ["sliding", "token", "per_worker"],
    ids=["Sliding Window", "Token Bucket", "Per Worker Token Bucket"],
)
async def test_with_ratelimiter(rl_name: str):
    limiters = {
        "sliding": SlidingRateLimiter(5, 0.2),
        "token": TokenBucketRateLimiter(5, 0.2),
        # We have 11 tasks for 10 workers, so per worker rate limiting should allow it
        # at rate of 1 request per 0.4 seconds (for this test to pass)
        "per_worker": PerWorkerRateLimiter(1, 0.4),
    }
    rate_limiter = limiters[rl_name]
    aqute = Aqute(
        workers_count=10, handle_coro=non_failing_handler, rate_limiter=rate_limiter
    )

    start = perf_counter()
    await aqute.apply_to_all(range(11))
    elapsed_time = perf_counter() - start

    assert 0.4 < elapsed_time < 0.5
