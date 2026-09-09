import asyncio

import pytest

from aqute import Aqute, AquteTaskTimeoutError


@pytest.mark.asyncio
@pytest.mark.parametrize("task_timeout", [None, 60])
async def test_handler_timeout_retries_when_selected(task_timeout):
    """A selected handler TimeoutError gets one retry and returns success."""
    calls = 0

    async def handler(value: int) -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise TimeoutError("upstream timed out")
        return value

    engine = Aqute(
        handler,
        1,
        task_timeout_seconds=task_timeout,
        retry_count=1,
        specific_errors_to_retry=TimeoutError,
    )
    (result,) = await engine.process_all([7])

    assert calls == 2
    assert result.unwrap() == 7


@pytest.mark.asyncio
@pytest.mark.parametrize("task_timeout", [None, 60])
async def test_handler_timeout_exclusion_preserves_original_error(task_timeout):
    """An excluded TimeoutError is delivered unchanged without another attempt."""
    calls = 0
    error = TimeoutError("upstream timed out")

    async def handler(value: int) -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise error
        return value

    engine = Aqute(
        handler,
        1,
        task_timeout_seconds=task_timeout,
        retry_count=1,
        errors_to_not_retry=TimeoutError,
    )
    (result,) = await engine.process_all([7])

    assert calls == 1
    assert result.error is error
    with pytest.raises(TimeoutError, match=r"^upstream timed out$") as caught:
        result.unwrap()
    assert caught.value is error


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("retry_error", "excluded_error", "expected_calls"),
    [
        (AquteTaskTimeoutError, None, 2),
        (AquteTaskTimeoutError, AquteTaskTimeoutError, 1),
        (TimeoutError, None, 1),
    ],
)
async def test_aqute_deadline_respects_its_retry_filters(
    retry_error, excluded_error, expected_calls
):
    """A real handler deadline uses AquteTaskTimeoutError for retry decisions."""
    calls = 0

    async def handler(value: int) -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            await asyncio.Event().wait()
        return value

    engine = Aqute(
        handler,
        1,
        task_timeout_seconds=0.01,
        retry_count=1,
        specific_errors_to_retry=retry_error,
        errors_to_not_retry=excluded_error,
    )
    async with asyncio.timeout(1):
        (result,) = await engine.process_all([7])

    assert calls == expected_calls
    if expected_calls == 2:
        assert result.unwrap() == 7
    else:
        with pytest.raises(AquteTaskTimeoutError, match="timed out"):
            result.unwrap()
