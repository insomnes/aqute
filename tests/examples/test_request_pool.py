import asyncio

import pytest

from aqute import Aqute
from examples.request_pool import request_pool


@pytest.mark.asyncio
async def test_routes_out_of_order_replies_none_and_errors():
    """Each caller receives its own outcome even when another finishes first."""
    started = asyncio.Event()
    release = asyncio.Event()

    async def handle(prompt: str) -> str | None:
        if prompt == "slow":
            started.set()
            await release.wait()
        if prompt == "invalid":
            raise ValueError("Invalid prompt")
        return None if prompt == "empty" else prompt.upper()

    async with (
        asyncio.timeout(1),
        request_pool(Aqute(handle, 2)) as ask,
        asyncio.TaskGroup() as callers,
    ):
        slow = callers.create_task(ask("slow"))
        await started.wait()
        assert await ask("fast") == "FAST"
        assert not slow.done()
        assert await ask("empty") is None
        with pytest.raises(ValueError, match="Invalid prompt"):
            await ask("invalid")
        release.set()
        assert await slow == "SLOW"
        assert await ask("later") == "LATER"
    with pytest.raises(RuntimeError, match="closed"):
        await ask("too late")


@pytest.mark.asyncio
async def test_cancelled_caller_does_not_cancel_admitted_work_or_other_callers():
    """Cancelling a waiter detaches its reply while the shared engine continues."""
    started = asyncio.Event()
    release = asyncio.Event()
    finished = asyncio.Event()

    async def handle(prompt: str) -> str:
        if prompt == "cancelled":
            started.set()
            try:
                await release.wait()
            finally:
                finished.set()
        return prompt

    async with asyncio.timeout(1), request_pool(Aqute(handle, 2)) as ask:
        caller = asyncio.create_task(ask("cancelled"))
        await started.wait()
        caller.cancel()
        with pytest.raises(asyncio.CancelledError):
            await caller
        assert not finished.is_set()
        assert await ask("unaffected") == "unaffected"
        release.set()
    assert finished.is_set()


@pytest.mark.asyncio
async def test_owner_deadline_cleans_up_handler_and_pending_caller():
    """A context deadline stops the worker and cancels the unresolved reply."""
    baseline = asyncio.all_tasks()
    started = asyncio.Event()
    finished = asyncio.Event()

    async def handle(prompt: str) -> str:
        started.set()
        try:
            await asyncio.Event().wait()
            return prompt
        finally:
            finished.set()

    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.1), request_pool(Aqute(handle, 1)) as ask:
            caller = asyncio.create_task(ask("pending"))
            await started.wait()
            await asyncio.Event().wait()
    with pytest.raises(asyncio.CancelledError):
        await caller
    assert finished.is_set()
    assert not (asyncio.all_tasks() - baseline)


@pytest.mark.asyncio
async def test_normal_exit_delivers_an_admitted_reply():
    """Normal shutdown drains the result before stopping its consumer."""
    started = asyncio.Event()
    release = asyncio.Event()

    async def handle(prompt: str) -> str:
        started.set()
        await release.wait()
        return prompt

    async with asyncio.timeout(1):
        async with request_pool(Aqute(handle, 1)) as ask:
            caller = asyncio.create_task(ask("admitted"))
            await started.wait()
            release.set()
        assert await caller == "admitted"


@pytest.mark.asyncio
@pytest.mark.parametrize("defer_cancellation", [False, True])
async def test_cancel_at_full_queue_release_keeps_later_replies_available(
    defer_cancellation,
):
    """Cancelling a waiting caller must not strand subsequent callers' results."""
    started = asyncio.Event()
    release = asyncio.Event()
    cancelled: asyncio.Task[str | None] | None = None

    async def handle(prompt: str) -> str:
        if prompt == "first":
            started.set()
            await release.wait()
        if prompt == "queued":
            assert cancelled is not None
            if defer_cancellation:
                asyncio.get_running_loop().call_soon(cancelled.cancel)
            else:
                cancelled.cancel()
            await asyncio.sleep(0)
        return prompt

    async with (
        asyncio.timeout(1),
        request_pool(Aqute(handle, 1)) as ask,
        asyncio.TaskGroup() as callers,
    ):
        first = callers.create_task(ask("first"))
        await started.wait()
        queued = callers.create_task(ask("queued"))
        await asyncio.sleep(0)
        cancelled = callers.create_task(ask("cancelled"))
        await asyncio.sleep(0)
        release.set()
        assert await first == "first"
        assert await queued == "queued"
        with pytest.raises(asyncio.CancelledError):
            await cancelled
        assert await ask("later") == "later"
