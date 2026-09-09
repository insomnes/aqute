"""Run an offline inference batch with token admission and application checkpoints."""

import asyncio
import json
import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from random import uniform
from time import monotonic

import httpx

from aqute import Aqute, AquteTask
from aqute.ratelimiter import PausableRateLimiter
from examples.http_client import RateLimitedError, retry_after_seconds

logger = logging.getLogger(__name__)
TOO_MANY_REQUESTS = 429


@dataclass(frozen=True)
class Prompt:
    text: str
    estimated_input_tokens: int
    max_tokens: int


class TokenCostLimiter:
    """Application-side token bucket; each attempt reserves its full estimated cost.

    Use on one event loop. The bucket starts full and holds one minute's budget.
    Failed attempts keep their reservation. No reconciliation with response usage.
    """

    def __init__(self, tokens_per_minute: int, cost: Callable[[Prompt], int]) -> None:
        if tokens_per_minute <= 0:
            raise ValueError("tokens_per_minute must be positive")
        self._capacity = tokens_per_minute
        self._rate = tokens_per_minute / 60
        self._cost = cost
        self._tokens = float(tokens_per_minute)
        self._updated = monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, name: str = "", task: AquteTask | None = None) -> None:
        if task is None:
            raise ValueError("TokenCostLimiter requires task data")
        cost = self._cost(task.data)
        if not 0 < cost <= self._capacity:
            raise ValueError("estimated cost must fit the positive token budget")
        async with self._lock:
            while True:
                now = monotonic()
                self._tokens = min(
                    self._capacity, self._tokens + (now - self._updated) * self._rate
                )
                self._updated = now
                if self._tokens >= cost:
                    self._tokens -= cost
                    return
                logger.debug("%s waiting for a %s-token reservation", name, cost)
                await asyncio.sleep((cost - self._tokens) / self._rate)


async def infer_batch(
    prompts: Iterable[Prompt],
    checkpoint: dict[Prompt, str],
    *,
    transport: httpx.AsyncBaseTransport | None = None,
) -> int:
    """Skip checkpointed inputs; save each successful output; raise terminal errors."""
    limiter = PausableRateLimiter(
        TokenCostLimiter(
            tokens_per_minute=600,
            cost=lambda prompt: prompt.estimated_input_tokens + prompt.max_tokens,
        )
    )
    # No SDK; disable HTTP transport retries so Aqute sees each attempt.
    if transport is None:
        transport = httpx.AsyncHTTPTransport(retries=0)
    async with httpx.AsyncClient(transport=transport, timeout=5.0) as client:

        async def infer(prompt: Prompt) -> str:
            response = await client.post(
                "https://inference.example.test/v1/chat/completions",
                json={
                    "model": "your-model-name",
                    "messages": [{"role": "user", "content": prompt.text}],
                    "max_tokens": prompt.max_tokens,
                },
            )
            if response.status_code == TOO_MANY_REQUESTS:
                delay = retry_after_seconds(response.headers.get("Retry-After"))
                limiter.pause_for(delay)
                logger.info("HTTP 429: pause admission for %.2fs", delay)
                raise RateLimitedError(
                    "inference throttled", request=response.request, response=response
                )
            response.raise_for_status()
            body = response.json()
            logger.info("Reported token usage: %s", body["usage"]["total_tokens"])
            return body["choices"][0]["message"]["content"]

        engine = Aqute(
            infer,
            workers_count=3,
            rate_limiter=limiter,
            retry_count=2,
            specific_errors_to_retry=(httpx.TransportError, RateLimitedError),
            retry_delay=lambda _attempt, _error: 0.1,
        )
        pending = (prompt for prompt in prompts if prompt not in checkpoint)
        completed = 0
        async with engine.iter_results(pending) as results:
            async for task in results:
                output = task.unwrap()
                # Application-owned checkpoint: replace with durable persistence.
                checkpoint[task.data] = output
                completed += 1
                logger.info("Completed %r: %s", task.data.text, output)
        return completed


async def main() -> int:
    prompts = [
        Prompt("Classify: service is healthy", 10, 8),
        Prompt("Classify: connection refused", 12, 8),
        Prompt("Classify: deployment completed", 10, 8),
    ]
    throttled_once = False

    async def respond(request: httpx.Request) -> httpx.Response:
        nonlocal throttled_once
        await asyncio.sleep(uniform(0.025, 0.1))  # Simulate remote inference.
        if not throttled_once:
            throttled_once = True
            return httpx.Response(429, headers={"Retry-After": "1"})
        text = json.loads(request.content)["messages"][0]["content"]
        label = "error" if "refused" in text else "ok"
        return httpx.Response(
            200,
            json={
                "choices": [{"message": {"role": "assistant", "content": label}}],
                "usage": {
                    "prompt_tokens": 8,
                    "completion_tokens": 1,
                    "total_tokens": 9,
                },
            },
        )

    checkpoint: dict[Prompt, str] = {}
    completed = await infer_batch(
        prompts, checkpoint, transport=httpx.MockTransport(respond)
    )
    resumed = await infer_batch(
        prompts, checkpoint, transport=httpx.MockTransport(respond)
    )
    logger.info("Resume: %s new completions", resumed)
    return completed


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("Results: %s completions", asyncio.run(main()))
