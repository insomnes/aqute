from typing import Optional

import pytest

from aqute import Aqute, AquteTask
from aqute.ratelimiter import RateLimiter


class StoringRateLimiter(RateLimiter):
    def __init__(self) -> None:
        self.store = []

    async def acquire(
        self, name: str = "", task: Optional[AquteTask] = None, **kwargs
    ) -> None:
        self.store.append((name, task.data, task.task_id, kwargs))


@pytest.mark.asyncio
async def test_rate_limiter_protocol():
    answer = [
        ("worker_0", 1, "0", {}),
        ("worker_0", 2, "1", {}),
    ]

    async def handler(i: int) -> str:
        return f"Result {i}"

    aq = Aqute(handler, 1, rate_limiter=StoringRateLimiter())
    await aq.apply_to_all([1, 2])
    assert aq._rate_limiter.store == answer
