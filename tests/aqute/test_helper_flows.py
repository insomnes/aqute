import asyncio
from typing import Any, Callable, Coroutine

import pytest

from aqute import Aqute


async def non_failing_handler(i: int) -> str:
    await asyncio.sleep(0.01)
    return f"Success {i}"


def get_specific_failing_handler() -> Callable[[Any], Coroutine[Any, Any, Any]]:
    call_counter = dict()

    async def specific_failing_handler(i: int) -> str:
        if i not in call_counter:
            call_counter[i] = 0
        call_counter[i] += 1

        if i == 7 and call_counter[i] < 3:
            raise ValueError("It was seven!")

        if i == 9:
            raise KeyError("NOT_OKEY")

        return await non_failing_handler(i)

    return specific_failing_handler


async def add_tasks(engine: Aqute, n: int):
    for i in range(n):
        await engine.add_task(i)


@pytest.mark.asyncio
async def test_apply_to_each():
    aqute = Aqute(
        workers_count=2,
        handle_coro=get_specific_failing_handler(),
        retry_count=2,
    )
    results = []

    async for r in aqute.apply_to_each(range(10)):
        results.append(r)

    successes = [t for t in results if t.success]
    assert len(successes) == 9

    fails = [t for t in results if not t.success]
    assert len(fails) == 1


@pytest.mark.asyncio
async def test_apply_to_all():
    aqute = Aqute(
        workers_count=2,
        handle_coro=get_specific_failing_handler(),
        retry_count=2,
    )
    results = await aqute.apply_to_all(range(10))

    successes = [t for t in results if t.success]
    assert len(successes) == 9

    fails = [t for t in results if not t.success]
    assert len(fails) == 1

    # Testing results are sorted as input
    assert [t.task_id for t in results] == [str(i) for i in range(10)]
