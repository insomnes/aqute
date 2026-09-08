import asyncio
from time import monotonic

import pytest

from aqute import Aqute
from aqute.ratelimiter import (
    PausableRateLimiter,
    PerWorkerRateLimiter,
    TokenBucketRateLimiter,
)
from aqute.task import AquteTask


def test_deadline_is_read_only_and_never_shortened():
    """A later request cannot undo an existing pause through the public API."""
    limiter = PausableRateLimiter(TokenBucketRateLimiter(1))
    assert limiter.paused_until == 0
    before = monotonic()
    limiter.pause_for(1)
    first = limiter.paused_until
    assert before + 1 <= first <= monotonic() + 1
    limiter.pause_for(0)
    limiter.pause_until(first - 0.5)
    assert limiter.paused_until == first
    limiter.pause_until(first + 1)
    assert limiter.paused_until == first + 1
    with pytest.raises(AttributeError):
        setattr(limiter, "paused_until", 0)  # noqa: B010 - exercise runtime rejection


@pytest.mark.parametrize("value", [-1, float("inf"), -float("inf"), float("nan")])
@pytest.mark.parametrize("method", ["pause_for", "pause_until"])
def test_invalid_pause_leaves_existing_deadline_unchanged(method, value):
    """Invalid caller input raises ValueError without cancelling a valid pause."""
    limiter = PausableRateLimiter(TokenBucketRateLimiter(1))
    limiter.pause_for(1)
    deadline = limiter.paused_until
    with pytest.raises(ValueError, match="finite and nonnegative"):
        getattr(limiter, method)(value)
    assert limiter.paused_until == deadline


@pytest.mark.asyncio
async def test_expired_deadline_does_not_block_and_remains_visible():
    limiter = PausableRateLimiter(TokenBucketRateLimiter(1))
    limiter.pause_until(monotonic() - 1)
    deadline = limiter.paused_until
    async with asyncio.timeout(1):
        await limiter.acquire()
    assert limiter.paused_until == deadline


@pytest.mark.asyncio
async def test_pause_blocks_all_workers_outside_handler_timeout():
    """Every handler starts after the pause, which cannot consume its timeout."""
    limiter = PausableRateLimiter(TokenBucketRateLimiter(8, allow_burst=True))
    limiter.pause_for(0.06)
    deadline = limiter.paused_until

    async def handle(value: int) -> tuple[int, float]:
        return value, monotonic()

    engine = Aqute(
        handle, workers_count=8, rate_limiter=limiter, task_timeout_seconds=0.01
    )
    async with asyncio.timeout(1):
        results = await engine.process_all(range(8))
    assert [task.unwrap()[0] for task in results] == list(range(8))
    assert all(task.unwrap()[1] >= deadline for task in results)


@pytest.mark.asyncio
async def test_pause_set_during_inner_acquisition_is_honored():
    """A grant in progress cannot admit an attempt before a newly set deadline."""
    entered = asyncio.Event()
    release = asyncio.Event()
    task = AquteTask(data="payload", task_id="request")
    received = []

    class WaitingLimiter:
        async def acquire(self, name="", task=None):
            received.append((name, task))
            entered.set()
            await release.wait()

    limiter = PausableRateLimiter(WaitingLimiter())
    async with asyncio.timeout(1):
        async with asyncio.TaskGroup() as group:
            acquisition = group.create_task(limiter.acquire(name="worker", task=task))
            await entered.wait()
            limiter.pause_for(0.06)
            deadline = limiter.paused_until
            release.set()
            await acquisition
            assert monotonic() >= deadline
    assert received == [("worker", task)]


@pytest.mark.asyncio
async def test_extension_while_waiting_delays_acquisition():
    """A sleeping acquisition must use an extension, not its original deadline."""
    limiter = PausableRateLimiter(TokenBucketRateLimiter(1))
    limiter.pause_for(0.04)
    async with asyncio.timeout(1):
        async with asyncio.TaskGroup() as group:
            acquisition = group.create_task(limiter.acquire())
            await asyncio.sleep(0)
            limiter.pause_for(0.08)
            deadline = limiter.paused_until
            await acquisition
            assert monotonic() >= deadline


@pytest.mark.asyncio
async def test_cancellation_during_pause_does_not_cancel_other_waiters():
    """Cancelling one caller propagates and preserves the shared pause for others."""
    limiter = PausableRateLimiter(TokenBucketRateLimiter(2, allow_burst=True))
    limiter.pause_for(0.06)
    deadline = limiter.paused_until
    async with asyncio.timeout(1):
        async with asyncio.TaskGroup() as group:
            cancelled = group.create_task(limiter.acquire())
            remaining = group.create_task(limiter.acquire())
            await asyncio.sleep(0)
            cancelled.cancel()
            with pytest.raises(asyncio.CancelledError):
                await cancelled
            await remaining
            assert monotonic() >= deadline
    assert limiter.paused_until == deadline


@pytest.mark.asyncio
async def test_token_bucket_composition_preserves_rate_after_initial_pause():
    """Normal sequential grants still obey the inner token bucket's interval."""
    limiter = PausableRateLimiter(TokenBucketRateLimiter(1, time_period=0.05))
    limiter.pause_for(0.03)
    async with asyncio.timeout(1):
        await limiter.acquire()
        first = monotonic()
        await limiter.acquire()
        assert monotonic() - first >= 0.049


@pytest.mark.asyncio
async def test_per_worker_composition_preserves_independent_worker_limits():
    """One worker's depleted quota must not block a different worker."""
    limiter = PausableRateLimiter(PerWorkerRateLimiter(1, time_period=0.08))
    limiter.pause_for(0.03)
    async with asyncio.timeout(1):
        await limiter.acquire("first")
        first = monotonic()
        async with asyncio.TaskGroup() as group:
            waiting = group.create_task(limiter.acquire("first"))
            await asyncio.sleep(0)
            await limiter.acquire("second")
            assert not waiting.done()
            await waiting
            assert monotonic() - first >= 0.079


@pytest.mark.asyncio
async def test_throttled_attempt_still_consumes_retry_budget():
    """Repeated throttling exhausts retry_count even when each failure pauses."""
    limiter = PausableRateLimiter(TokenBucketRateLimiter(1000))
    attempts = 0

    async def handle(_value: int) -> None:
        nonlocal attempts
        attempts += 1
        limiter.pause_for(0.01)
        raise ValueError("throttled")

    engine = Aqute(handle, workers_count=1, rate_limiter=limiter, retry_count=1)
    results = await engine.process_all([1])
    with pytest.raises(ValueError, match="throttled"):
        results[0].unwrap()
    assert attempts == 2
