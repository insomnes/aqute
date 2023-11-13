import asyncio
from typing import Any, Optional

import pytest

from aqute import Aqute, AquteError


async def non_failing_handler(task: Any) -> Any:
    await asyncio.sleep(0.01)
    return task


@pytest.mark.asyncio
async def test_start_timeout():
    aqute = Aqute(
        workers_count=2,
        handle_coro=non_failing_handler,
        start_timeout_seconds=0.1,
    )
    with pytest.raises(AquteError) as exc:
        async with aqute:
            await asyncio.sleep(0.12)
    assert str(exc.value).startswith("Waited")


@pytest.mark.asyncio
async def test_wait_on_empty_load():
    aqute = Aqute(workers_count=2, handle_coro=non_failing_handler)
    with pytest.raises(AquteError) as exc:
        await aqute.wait_till_end()
    assert str(exc.value).startswith("Cannot")


async def aq_wait_coro(timeout: Optional[float]):
    aqute = Aqute(
        workers_count=2,
        start_timeout_seconds=timeout,
        handle_coro=non_failing_handler,
        retry_count=0,
    )
    async with aqute:
        await asyncio.sleep(0.7)


@pytest.mark.asyncio
async def test_not_raising_on_none_timeout():
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(aq_wait_coro(None), timeout=0.5)


@pytest.mark.asyncio
async def test_raising_with_timeout():
    with pytest.raises(AquteError):
        await asyncio.wait_for(aq_wait_coro(0.2), timeout=0.5)
