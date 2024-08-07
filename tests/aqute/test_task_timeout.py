import asyncio
from typing import NamedTuple, Optional

import pytest

from aqute import Aqute, AquteTaskTimeoutError


class TaskTimeoutTestCase(NamedTuple):
    name: str
    task_timeout: Optional[float]

    expected_success: int
    expected_errors: int

    retry_count: int
    retry_on_timeout: bool


TASK_TIMEOUT_TEST_CASES = [
    TaskTimeoutTestCase(
        name="no-timeout",
        task_timeout=None,
        expected_success=10,
        expected_errors=0,
        retry_count=0,
        retry_on_timeout=False,
    ),
    TaskTimeoutTestCase(
        name="Timeout with retry count 0, retry allowed",
        task_timeout=0.06,
        expected_success=5,
        expected_errors=5,
        retry_count=0,
        retry_on_timeout=True,
    ),
    TaskTimeoutTestCase(
        name="Timeout with retry count 1, retry allowed",
        task_timeout=0.06,
        expected_success=10,
        expected_errors=0,
        retry_count=1,
        retry_on_timeout=True,
    ),
    TaskTimeoutTestCase(
        name="Timeout with retry count 1, retry not allowed",
        task_timeout=0.06,
        expected_success=5,
        expected_errors=5,
        retry_count=1,
        retry_on_timeout=False,
    ),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("case", TASK_TIMEOUT_TEST_CASES, ids=lambda c: c.name)
async def test_timeout(case: TaskTimeoutTestCase):
    def get_retriable_handler():
        call_count = {}

        async def slow_handler(data: int) -> str:
            if data not in call_count:
                call_count[data] = 0

            call_count[data] += 1
            if call_count[data] > 1:
                return f"handled-{data}"

            await asyncio.sleep(0.01 * data)
            return f"handled-{data}"

        return slow_handler

    errors_to_not_retry = None if case.retry_on_timeout else AquteTaskTimeoutError

    aqute = Aqute(
        handle_coro=get_retriable_handler(),
        workers_count=2,
        task_timeout_seconds=case.task_timeout,
        errors_to_not_retry=errors_to_not_retry,
        retry_count=case.retry_count,
    )

    total = case.expected_success + case.expected_errors

    for i in range(1, total + 1):
        await aqute.add_task(i)

    async with aqute:
        await aqute.wait_till_end()

    handled_tasks = aqute.extract_all_results()
    assert len(handled_tasks) == total

    errors = [t.error for t in handled_tasks if not t.success]
    assert len(errors) == case.expected_errors
    assert all([isinstance(e, AquteTaskTimeoutError) for e in errors])

    successful_tasks = [t for t in handled_tasks if t.success]
    assert len(successful_tasks) == case.expected_success
