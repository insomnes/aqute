import asyncio
from collections.abc import Coroutine
from typing import (
    Any,
    Callable,
    NamedTuple,
    Optional,
    Union,
)

import pytest

from aqute import Aqute, AquteTask


async def non_failing_handler(i: int) -> str:
    await asyncio.sleep(0.01)
    return f"Success {i}"


class ShouldRetryTestCase(NamedTuple):
    name: str

    specific_errors_to_retry: Optional[
        Union[tuple[type[Exception], ...], type[Exception]]
    ]
    errors_to_not_retry: Optional[Union[tuple[type[Exception], ...], type[Exception]]]
    task: AquteTask

    expected: bool


SHOULD_RETRY_TEST_CASES = [
    ShouldRetryTestCase(
        name="No specific errors, no retries left",
        specific_errors_to_retry=None,
        errors_to_not_retry=None,
        task=AquteTask(1, "1", error=ValueError("Ooops"), _remaining_tries=0),
        expected=False,
    ),
    ShouldRetryTestCase(
        name="No specific errors, retries left",
        specific_errors_to_retry=None,
        errors_to_not_retry=None,
        task=AquteTask(1, "1", error=ValueError("Ooops"), _remaining_tries=1),
        expected=True,
    ),
    ShouldRetryTestCase(
        name="Specific error, matches",
        specific_errors_to_retry=ValueError,
        errors_to_not_retry=None,
        task=AquteTask(1, "1", error=ValueError("Ooops"), _remaining_tries=1),
        expected=True,
    ),
    ShouldRetryTestCase(
        name="Specific error, no retrires left",
        specific_errors_to_retry=ValueError,
        errors_to_not_retry=None,
        task=AquteTask(1, "1", error=ValueError("Ooops"), _remaining_tries=0),
        expected=False,
    ),
    ShouldRetryTestCase(
        name="Specific error, matches (tuple)",
        specific_errors_to_retry=(ValueError, KeyError),
        errors_to_not_retry=None,
        task=AquteTask(1, "1", error=ValueError("Ooops"), _remaining_tries=1),
        expected=True,
    ),
    ShouldRetryTestCase(
        name="Do not retry, matches",
        specific_errors_to_retry=None,
        errors_to_not_retry=ValueError,
        task=AquteTask(1, "1", error=ValueError("Ooops"), _remaining_tries=1),
        expected=False,
    ),
    ShouldRetryTestCase(
        name="Do not retry, not matches",
        specific_errors_to_retry=None,
        errors_to_not_retry=KeyError,
        task=AquteTask(1, "1", error=ValueError("Ooops"), _remaining_tries=1),
        expected=True,
    ),
    ShouldRetryTestCase(
        name="Do not retry, matches (tuple)",
        specific_errors_to_retry=None,
        errors_to_not_retry=(ValueError, KeyError),
        task=AquteTask(1, "1", error=ValueError("Ooops"), _remaining_tries=1),
        expected=False,
    ),
    ShouldRetryTestCase(
        name="Do not retry prevails",
        specific_errors_to_retry=(ValueError, KeyError),
        errors_to_not_retry=(ValueError),
        task=AquteTask(1, "1", error=ValueError("Ooops"), _remaining_tries=1),
        expected=False,
    ),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    SHOULD_RETRY_TEST_CASES,
    ids=[case.name for case in SHOULD_RETRY_TEST_CASES],
)
async def test_should_retry_task_helper(case: ShouldRetryTestCase):
    aqute = Aqute(
        handle_coro=non_failing_handler,
        workers_count=2,
        specific_errors_to_retry=case.specific_errors_to_retry,
        errors_to_not_retry=case.errors_to_not_retry,
        # Should not matter for this test
        retry_count=0,
    )

    assert aqute._should_retry_task(case.task) == case.expected


class AlwaysFailError(Exception):
    index: int = 9
    call_to_stop_fail: int = 999


class OneFailError(Exception):
    index: int = 8
    call_to_stop_fail: int = 2


class TwoFailError(Exception):
    index: int = 7
    call_to_stop_fail: int = 3


def get_specific_failing_handler() -> Callable[[Any], Coroutine[Any, Any, Any]]:
    call_counter = dict()

    async def specific_failing_handler(i: int) -> str:
        if i not in call_counter:
            call_counter[i] = 0
        call_counter[i] += 1

        for e in [AlwaysFailError, OneFailError, TwoFailError]:
            if i == e.index and call_counter[i] < e.call_to_stop_fail:
                raise e(f"I am {e.__name__}")

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
) -> list[AquteTask]:
    results = aqute.extract_all_results()
    successes = [t for t in results if t.success]
    fails = [t for t in results if not t.success]
    assert len(successes) == success_count and len(fails) == fails_count
    return results


class RetryTestCase(NamedTuple):
    name: str
    retry_count: int
    specific_errors_to_retry: Optional[
        Union[tuple[type[Exception], ...], type[Exception]]
    ]
    errors_to_not_retry: Optional[Union[tuple[type[Exception], ...], type[Exception]]]
    expected_successes: int
    expected_fails: int

    expected_one_failed: int
    one_remaining_tries: int

    expected_two_failed: int
    two_remaining_tries: int

    expected_always_failed: int
    always_remaining_tries: int


# 1 task will always fail
# 1 task will fail 1 time and then succeed
# 1 task will fail 2 times and then succeed on 3rd try
# Other tasks will succeed
RETRY_TEST_CASES = [
    RetryTestCase(
        name="No retries",
        retry_count=0,
        specific_errors_to_retry=None,
        errors_to_not_retry=None,
        expected_successes=7,
        expected_fails=3,
        expected_one_failed=1,
        one_remaining_tries=0,
        expected_two_failed=1,
        two_remaining_tries=0,
        expected_always_failed=1,
        always_remaining_tries=0,
    ),
    RetryTestCase(
        name="Retry works on 1",
        retry_count=1,
        specific_errors_to_retry=None,
        errors_to_not_retry=None,
        expected_successes=8,
        expected_fails=2,
        expected_one_failed=0,
        one_remaining_tries=0,
        expected_two_failed=1,
        two_remaining_tries=0,
        expected_always_failed=1,
        always_remaining_tries=0,
    ),
    RetryTestCase(
        name="Retry works on 2",
        retry_count=2,
        specific_errors_to_retry=None,
        errors_to_not_retry=None,
        expected_successes=9,
        expected_fails=1,
        expected_one_failed=0,
        one_remaining_tries=1,
        expected_two_failed=0,
        two_remaining_tries=0,
        expected_always_failed=1,
        always_remaining_tries=0,
    ),
    RetryTestCase(
        name="Retry specific error",
        retry_count=2,
        specific_errors_to_retry=OneFailError,
        errors_to_not_retry=None,
        expected_successes=8,
        expected_fails=2,
        expected_one_failed=0,
        one_remaining_tries=1,
        expected_two_failed=1,
        two_remaining_tries=2,
        expected_always_failed=1,
        always_remaining_tries=2,
    ),
    RetryTestCase(
        name="Retry two specific errors",
        retry_count=2,
        specific_errors_to_retry=(OneFailError, TwoFailError),
        errors_to_not_retry=None,
        expected_successes=9,
        expected_fails=1,
        expected_one_failed=0,
        one_remaining_tries=1,
        expected_two_failed=0,
        two_remaining_tries=0,
        expected_always_failed=1,
        always_remaining_tries=2,
    ),
    RetryTestCase(
        name="Do not retry specific error",
        retry_count=2,
        specific_errors_to_retry=None,
        errors_to_not_retry=OneFailError,
        expected_successes=8,
        expected_fails=2,
        expected_one_failed=1,
        one_remaining_tries=2,
        expected_two_failed=0,
        two_remaining_tries=0,
        expected_always_failed=1,
        always_remaining_tries=0,
    ),
    RetryTestCase(
        name="Do not retry prevails",
        retry_count=2,
        specific_errors_to_retry=(OneFailError, TwoFailError),
        errors_to_not_retry=(OneFailError),
        expected_successes=8,
        expected_fails=2,
        expected_one_failed=1,
        one_remaining_tries=2,
        expected_two_failed=0,
        two_remaining_tries=0,
        expected_always_failed=1,
        always_remaining_tries=2,
    ),
    RetryTestCase(
        name="Retry specifics errors do not match",
        retry_count=2,
        specific_errors_to_retry=ValueError,
        errors_to_not_retry=KeyError,
        expected_successes=7,
        expected_fails=3,
        expected_one_failed=1,
        one_remaining_tries=2,
        expected_two_failed=1,
        two_remaining_tries=2,
        expected_always_failed=1,
        always_remaining_tries=2,
    ),
    RetryTestCase(
        name="No retries, specific error set",
        retry_count=0,
        specific_errors_to_retry=OneFailError,
        errors_to_not_retry=None,
        expected_successes=7,
        expected_fails=3,
        expected_one_failed=1,
        one_remaining_tries=0,
        expected_two_failed=1,
        two_remaining_tries=0,
        expected_always_failed=1,
        always_remaining_tries=0,
    ),
    RetryTestCase(
        name="High retry count",
        retry_count=100,
        specific_errors_to_retry=None,
        errors_to_not_retry=None,
        expected_successes=9,
        expected_fails=1,
        expected_one_failed=0,
        one_remaining_tries=99,
        expected_two_failed=0,
        two_remaining_tries=98,
        expected_always_failed=1,
        always_remaining_tries=0,
    ),
    RetryTestCase(
        name="EXTRA High retry count",
        retry_count=1_000,
        specific_errors_to_retry=None,
        errors_to_not_retry=None,
        expected_successes=10,
        expected_fails=0,
        expected_one_failed=0,
        one_remaining_tries=9_999,
        expected_two_failed=0,
        two_remaining_tries=9_998,
        expected_always_failed=0,
        always_remaining_tries=0,
    ),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    RETRY_TEST_CASES,
    ids=[case.name for case in RETRY_TEST_CASES],
)
async def test_retry(case: RetryTestCase):
    aqute = Aqute(
        workers_count=2,
        handle_coro=get_specific_failing_handler(),
        retry_count=case.retry_count,
        specific_errors_to_retry=case.specific_errors_to_retry,
        errors_to_not_retry=case.errors_to_not_retry,
    )
    async with aqute:
        await add_tasks(aqute, case.expected_successes + case.expected_fails)
        await aqute.wait_till_end()

    res = check_susccess_and_fails(aqute, case.expected_successes, case.expected_fails)

    one_failed = [
        t
        for t in res
        if t.error and t.error.index == OneFailError.index  # type: ignore
    ]
    assert len(one_failed) == case.expected_one_failed
    if case.expected_one_failed > 0:
        assert one_failed[0]._remaining_tries == case.one_remaining_tries

    two_failed = [
        t
        for t in res
        if t.error and t.error.index == TwoFailError.index  # type: ignore
    ]
    assert len(two_failed) == case.expected_two_failed
    if case.expected_two_failed > 0:
        assert two_failed[0]._remaining_tries == case.two_remaining_tries

    always_failed = [
        t
        for t in res
        if t.error and t.error.index == AlwaysFailError.index  # type: ignore
    ]
    assert len(always_failed) == case.expected_always_failed
    if case.expected_always_failed > 0:
        assert always_failed[0]._remaining_tries == case.always_remaining_tries
