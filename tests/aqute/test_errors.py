import asyncio
from typing import Any, Optional

import pytest

from aqute import Aqute, AquteError, AquteTooManyTasksFailedError


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


async def failing_handler(task: int) -> int:
    await asyncio.sleep(0.01)
    if task % 2 == 0:
        raise ValueError("Even task number")
    return task


@pytest.mark.asyncio
@pytest.mark.parametrize("retry_count", [0, 1], ids=["no_retry", "one_retry"])
async def test_too_many_failed_tasks_error(retry_count: int):
    aqute = Aqute(
        workers_count=2,
        handle_coro=failing_handler,
        retry_count=retry_count,
        total_failed_tasks_limit=2,
    )
    for i in range(1, 6):
        await aqute.add_task(i)

    with pytest.raises(AquteTooManyTasksFailedError) as exc:
        async with aqute:
            await aqute.wait_till_end()
    assert "limit reached: 2" in str(exc.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("retry_count", [0, 1], ids=["no_retry", "one_retry"])
async def test_not_enough_failed_tasks_for_error(retry_count: int):
    aqute = Aqute(
        workers_count=2,
        handle_coro=failing_handler,
        retry_count=retry_count,
        total_failed_tasks_limit=3,
    )
    for i in range(1, 6):
        await aqute.add_task(i)

    async with aqute:
        await aqute.wait_till_end()
