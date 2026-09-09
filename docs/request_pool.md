# Per-caller request pool

Use one long-lived engine when independent callers each need their own reply.
The pool yields an `ask(prompt)` function: concurrent calls share workers, queues,
and a rate limiter, but each awaits the outcome matching its own task ID.
The same pool can serve later calls without restarting the engine.

Run the offline example from a development checkout:

```bash
uv run --locked python -m examples.request_pool
```

Install Aqute 0.10.1 or newer before using this recipe in an application:

```bash
python -m pip install aqute==0.10.1
```

The released package includes the bounded-submission cancellation fix missing
from 0.10.0. Copy the source below into your application; the `examples` package
is not installed with Aqute. A library checkout is not required.

The simulated handler sleeps for 25–100 ms. It returns a valid `None` for
`empty`, raises a terminal `ValueError` for `invalid`, and throttles `retry` once.
The throttle pauses new attempts for 150 ms before raising the retryable error.
The demonstration collects four initial outcomes and one later reply: four
successes, including `None`, and one failure. No credentials or server are needed.

## Runnable source

<!-- example: examples/request_pool.py -->
```python
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
```
<!-- /example -->

## Ownership and shutdown

Pass a fresh engine and give this context exclusive ownership of its lifetime
and result queue. Configure the handler, retry policy and limiter on that engine.
The example starts it synchronously and keeps one consumer running until draining
finishes. It registers each future before submission can yield, and stores the
terminal task in that future. Only the matching caller calls `unwrap()`, which
preserves `None` and raises that caller's terminal handler error.

Stop producers and await their request tasks before leaving the context, as the
demonstration does. Normal exit rejects later calls, finishes admitted work and
drains replies. The default input capacity equals `workers_count`; excess callers
can wait inside `add_task()`. Bound caller tasks or give admission a deadline;
finite engine queues do not bound the application's caller count.

Cancelling a caller removes its reply future. It does not cancel work already
admitted to Aqute or undo remote side effects. Other callers continue normally.
An exception or cancellation of the context owner cancels processing and cleans
up pending futures; callers still waiting for admission can receive `AquteError`.
The application must await its caller tasks too. Use an outer `asyncio.timeout`
when the whole operation needs a deadline. Cleanup requires cooperative handlers.
Unexpected processing failures propagate and can include an `ExceptionGroup`.
Ordinary exceptions leaving the context body, including an unhandled `ask()` error,
can also propagate as an `ExceptionGroup` from the background `TaskGroup`.
Handle individual request errors inside the context, or use `except*` outside it.

This context manager is example code, not an installed Aqute API or a durable
request service. Copy it into application code and own the real client's lifetime.
For real HTTP errors and `Retry-After`, see the [HTTP recipe](http_client.md);
for token budgets and SDK retry ownership, see [LLM inference](llm_inference.md).
