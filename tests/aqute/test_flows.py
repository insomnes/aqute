import asyncio
from collections.abc import Callable, Coroutine
from typing import Any

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


async def failing_handler(i: int) -> str:
    if i != 0 and i % 7 == 0:
        raise ValueError("It was seven!")
    return await non_failing_handler(i)


async def add_tasks(engine: Aqute, n: int):
    for i in range(n):
        await engine.add_task(i)


def check_susccess(aqute: Aqute, should_be: int):
    successes = [t for t in aqute.drain_results() if t.success]
    assert len(successes) == should_be


def check_susccess_and_fails(
    aqute: Aqute, success_count: int, fails_count
) -> list[AquteTask]:
    results = aqute.drain_results()
    successes = [t for t in results if t.success]
    fails = [t for t in results if not t.success]
    assert len(successes) == success_count and len(fails) == fails_count
    return results


@pytest.mark.asyncio
async def test_most_verbose_way():
    aqute = Aqute(
        workers_count=2,
        handle_coro=non_failing_handler,
        input_task_queue_size=0,
        result_queue=asyncio.Queue(0),
    )
    await add_tasks(aqute, 10)
    aqute.finish_submitting()
    await aqute.start()
    await aqute.stop()

    check_susccess(aqute, 10)


@pytest.mark.asyncio
async def test_simple_way():
    aqute = Aqute(
        workers_count=2, handle_coro=non_failing_handler, result_queue=asyncio.Queue(0)
    )
    async with aqute:
        await add_tasks(aqute, 10)
        await aqute.finish()

    check_susccess(aqute, 10)


@pytest.mark.asyncio
async def test_simple_way_add_before():
    aqute = Aqute(
        workers_count=2,
        handle_coro=non_failing_handler,
        input_task_queue_size=0,
        result_queue=asyncio.Queue(0),
    )
    await add_tasks(aqute, 10)
    async with aqute:
        await aqute.finish()

    check_susccess(aqute, 10)


@pytest.mark.asyncio
async def test_simple_verbose():
    aqute = Aqute(
        workers_count=2,
        handle_coro=non_failing_handler,
        input_task_queue_size=0,
        result_queue=asyncio.Queue(0),
    )
    await add_tasks(aqute, 10)
    await aqute.run()
    await aqute.stop()

    check_susccess(aqute, 10)


@pytest.mark.asyncio
async def test_start_without_await():
    aqute = Aqute(
        workers_count=2, handle_coro=non_failing_handler, result_queue=asyncio.Queue(0)
    )
    aqute.start()
    await add_tasks(aqute, 10)
    await aqute.finish()
    await aqute.stop()

    check_susccess(aqute, 10)


@pytest.mark.asyncio
async def test_failed_tasks():
    aqute = Aqute(
        workers_count=2, handle_coro=failing_handler, result_queue=asyncio.Queue(0)
    )
    async with aqute:
        await add_tasks(aqute, 10)
        await aqute.finish()

    check_susccess_and_fails(aqute, 9, 1)


@pytest.mark.asyncio
async def test_total_failed_tasks_limit_do_not_intervene():
    aqute = Aqute(
        workers_count=2,
        handle_coro=failing_handler,
        total_failed_tasks_limit=2,
        result_queue=asyncio.Queue(0),
    )
    async with aqute:
        await add_tasks(aqute, 10)
        await aqute.finish()

    check_susccess_and_fails(aqute, 9, 1)
