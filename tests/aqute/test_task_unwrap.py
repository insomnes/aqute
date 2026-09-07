from typing import assert_type

import pytest

from aqute import Aqute, AquteError, AquteTask


@pytest.mark.asyncio
async def test_unwrap_returns_typed_successful_value():
    """A completed result must retain the handler's int type and public outcome."""

    async def handle(value: int) -> int:
        return value * 2

    (task,) = await Aqute(handle, workers_count=1).process_all([21])

    assert assert_type(task.unwrap(), int) == 42
    assert task.data == 21
    assert task.result == 42
    assert task.error is None
    assert task.success


@pytest.mark.asyncio
async def test_unwrap_distinguishes_successful_none_from_pending():
    """Unwrap must distinguish successful None from an unchanged pending outcome."""
    pending = AquteTask[int, None](data=1, task_id="pending")

    with pytest.raises(AquteError, match="Task pending is not complete"):
        pending.unwrap()

    assert pending.data == 1
    assert pending.task_id == "pending"
    assert pending.result is None
    assert pending.error is None
    assert not pending.success

    async def handle(_value: int) -> None:
        return None

    (task,) = await Aqute(handle, workers_count=1).process_all([1])

    assert assert_type(task.unwrap(), None) is None
    assert task.data == 1
    assert task.result is None
    assert task.error is None
    assert task.success


@pytest.mark.asyncio
async def test_unwrap_raises_stored_error_after_batch_completion():
    """Unwrap must raise the stored error after the full batch has completed."""
    handled = []
    error = ValueError("handler failed")

    async def handle(value: int) -> int:
        handled.append(value)
        if value == 1:
            raise error
        return value * 2

    tasks = await Aqute(handle, workers_count=1).process_all([1, 2])

    assert handled == [1, 2]
    assert [task.data for task in tasks] == [1, 2]
    with pytest.raises(ValueError, match="handler failed") as caught:
        tasks[0].unwrap()
    assert caught.value is error
    assert tasks[0].error is error
    assert tasks[0].result is None
    assert not tasks[0].success
    assert tasks[1].unwrap() == 4
