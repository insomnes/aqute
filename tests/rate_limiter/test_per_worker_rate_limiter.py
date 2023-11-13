from time import perf_counter

import pytest

from aqute.ratelimiter import PerWorkerRateLimiter


@pytest.mark.asyncio
async def test_rate_limiters_divided():
    """
    We can acquire on different names almost instantly, and after that
    we can acquire again on the same names after period.
    """

    limiter = PerWorkerRateLimiter(1, 0.1)

    async def acquire_all():
        start = perf_counter()
        for i in range(10):
            await limiter.acquire(str(i))
        return perf_counter() - start

    assert await acquire_all() < 0.05
    assert 0.1 <= await acquire_all() < 0.15
