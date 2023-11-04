import asyncio
from time import perf_counter
from typing import Any, Callable, Coroutine, List, Optional

import pytest

from aqute import Aqute, AquteError, AquteTask
from aqute.ratelimiter import (
    PerWorkerRateLimiter,
    SlidingRateLimiter,
    TokenBucketRateLimiter,
)


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


@pytest.mark.asyncio
async def test_most_verbose_way():
    aqute = Aqute(workers_count=2, handle_coro=non_failing_handler)
    await add_tasks(aqute, 10)
    aqute.set_all_tasks_added()
    await aqute.start()
    await aqute.stop()

    check_susccess(aqute, 10)


@pytest.mark.asyncio
async def test_simple_way():
    aqute = Aqute(workers_count=2, handle_coro=non_failing_handler)
    async with aqute:
        await add_tasks(aqute, 10)
        await aqute.wait_till_end()

    check_susccess(aqute, 10)


@pytest.mark.asyncio
async def test_simple_way_add_before():
    aqute = Aqute(workers_count=2, handle_coro=non_failing_handler)
    await add_tasks(aqute, 10)
    async with aqute:
        await aqute.wait_till_end()

    check_susccess(aqute, 10)


@pytest.mark.asyncio
async def test_simple_verbose():
    aqute = Aqute(workers_count=2, handle_coro=non_failing_handler)
    await add_tasks(aqute, 10)
    await aqute.start_and_wait()
    await aqute.stop()

    check_susccess(aqute, 10)


@pytest.mark.asyncio
async def test_start_without_await():
    aqute = Aqute(workers_count=2, handle_coro=non_failing_handler)
    aqute.start()
    await add_tasks(aqute, 10)
    await aqute.wait_till_end()
    await aqute.stop()

    check_susccess(aqute, 10)


@pytest.mark.asyncio
async def test_failed_tasks():
    aqute = Aqute(workers_count=2, handle_coro=failing_handler)
    async with aqute:
        await add_tasks(aqute, 10)
        await aqute.wait_till_end()

    check_susccess_and_fails(aqute, 9, 1)


@pytest.mark.asyncio
async def test_retry_works():
    aqute = Aqute(workers_count=2, handle_coro=get_specific_failing_handler())
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
    )
    async with aqute:
        await add_tasks(aqute, 10)
        await aqute.wait_till_end()

    results = check_susccess_and_fails(aqute, 8, 2)

    val_result = [t for t in results if isinstance(t.error, ValueError)][0]
    assert val_result._remaining_tries == 2

    key_result = [t for t in results if isinstance(t.error, KeyError)][0]
    assert key_result._remaining_tries == 0


@pytest.mark.asyncio
async def test_apply_to_each():
    aqute = Aqute(workers_count=2, handle_coro=get_specific_failing_handler())
    results = []

    async for r in aqute.apply_to_each(range(10)):
        results.append(r)

    successes = [t for t in results if t.success]
    assert len(successes) == 9

    fails = [t for t in results if not t.success]
    assert len(fails) == 1


@pytest.mark.asyncio
async def test_apply_to_all():
    aqute = Aqute(workers_count=2, handle_coro=get_specific_failing_handler())
    results = await aqute.apply_to_all(range(10))

    successes = [t for t in results if t.success]
    assert len(successes) == 9

    fails = [t for t in results if not t.success]
    assert len(fails) == 1

    # Testing results are sorted as input
    assert [t.task_id for t in results] == [str(i) for i in range(10)]


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
