import asyncio
from typing import Any, Callable, Coroutine, List

import pytest

from aqute import Aqute, AquteTask


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


def check_susccess(aqute: Aqute, should_be: int):
    successes = [t for t in aqute.extract_all_results() if t.success]
    assert len(successes) == should_be


def check_susccess_and_fails(
    aqute: Aqute, success_count: int, fails_count
) -> List[AquteTask]:
    results = aqute.extract_all_results()
    successes = [t for t in results if t.success]
    fails = [t for t in results if not t.success]
    assert len(successes) == success_count and len(fails) == fails_count
    return results


@pytest.mark.asyncio
async def test_retry_works():
    aqute = Aqute(
        workers_count=2,
        handle_coro=get_specific_failing_handler(),
        retry_count=2,
    )
    async with aqute:
        await add_tasks(aqute, 10)
        await aqute.wait_till_end()

    check_susccess_and_fails(aqute, 9, 1)


@pytest.mark.asyncio
async def test_no_retries():
    aqute = Aqute(
        workers_count=2,
        handle_coro=get_specific_failing_handler(),
        retry_count=0,
    )
    async with aqute:
        await add_tasks(aqute, 10)
        await aqute.wait_till_end()

    check_susccess_and_fails(aqute, 8, 2)


@pytest.mark.asyncio
async def test_retriable_errors():
    aqute = Aqute(
        workers_count=2,
        handle_coro=get_specific_failing_handler(),
        specific_errors_to_retry=(KeyError,),
        retry_count=2,
    )
    async with aqute:
        await add_tasks(aqute, 10)
        await aqute.wait_till_end()

    results = check_susccess_and_fails(aqute, 8, 2)

    val_result = [t for t in results if isinstance(t.error, ValueError)][0]
    assert val_result._remaining_tries == 2

    key_result = [t for t in results if isinstance(t.error, KeyError)][0]
    assert key_result._remaining_tries == 0
