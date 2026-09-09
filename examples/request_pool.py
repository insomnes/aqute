"""Route replies from one long-lived engine to independent callers."""

import asyncio
import logging
from collections.abc import AsyncIterator, Callable, Coroutine
from contextlib import asynccontextmanager
from random import uniform
from typing import Any
from uuid import uuid4

from aqute import Aqute, AquteError, AquteTask
from aqute.ratelimiter import PausableRateLimiter, TokenBucketRateLimiter


@asynccontextmanager
async def request_pool(
    engine: Aqute[str, str | None],
) -> AsyncIterator[Callable[[str], Coroutine[Any, Any, str | None]]]:
    """Own a fresh engine; callers own their request tasks."""
    pending: dict[str, asyncio.Future[AquteTask[str, str | None]]] = {}
    accepting = True

    async def ask(prompt: str) -> str | None:
        if not accepting:
            raise RuntimeError("Request pool is closed")
        task_id = str(uuid4())
        future = asyncio.get_running_loop().create_future()
        pending[task_id] = future  # Register before submission can yield.
        try:
            await engine.add_task(prompt, task_id=task_id)
            return (await future).unwrap()
        finally:
            pending.pop(task_id, None)
            future.cancel()

    async def collect() -> None:
        while True:
            try:
                task = await engine.get_result()
            except AquteError:
                if not accepting:  # finish() ended the run during normal shutdown.
                    return
                raise
            future = pending.get(task.task_id)
            if future is not None and not future.done():
                # Store the outcome; only its caller unwraps a terminal error.
                future.set_result(task)

    engine.start()  # Synchronous: do not await the background processing task here.
    try:
        async with asyncio.TaskGroup() as background:
            background.create_task(collect())
            try:
                yield ask
            finally:
                accepting = False
            await engine.finish()  # Keep the collector running while draining.
    finally:
        try:
            await engine.stop()
        finally:
            for future in pending.values():
                future.cancel()
            pending.clear()


class ThrottledError(Exception):
    """Simulate a retryable remote throttle response."""


async def main() -> list[str | None | BaseException]:
    limiter = PausableRateLimiter(TokenBucketRateLimiter(max_rate=20))
    throttled = False

    async def handle(prompt: str) -> str | None:
        nonlocal throttled
        await asyncio.sleep(uniform(0.025, 0.1))
        if prompt == "retry" and not throttled:
            throttled = True
            limiter.pause_for(0.15)  # Inside the handler, before the retryable error.
            raise ThrottledError("Retry after 0.15 seconds")
        if prompt == "invalid":
            raise ValueError("Invalid prompt")
        return None if prompt == "empty" else f"Reply to {prompt}"

    engine = Aqute(
        handle,
        workers_count=3,
        rate_limiter=limiter,
        retry_count=2,
        specific_errors_to_retry=ThrottledError,
    )
    async with request_pool(engine) as ask:
        replies = await asyncio.gather(
            *(ask(prompt) for prompt in ("first", "empty", "invalid", "retry")),
            return_exceptions=True,
        )
        replies.append(await ask("later"))  # The same pool serves another caller.
    return replies


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    replies = asyncio.run(main())
    failures = sum(isinstance(reply, BaseException) for reply in replies)
    logging.info("Results: %s replies, %s failure", len(replies) - failures, failures)
