# LLM batch inference

Use Aqute for independent remote inference requests when the application needs
bounded concurrency, selected retries, and incremental results. This example adds
a token-cost budget to the [HTTP recipe](http_client.md). It uses HTTPX directly;
it does not require a vendor SDK, credentials, or a running model server.

From a development checkout, run it offline:

```bash
uv run --locked python -m examples.llm_inference
```

The mock endpoint returns chat-completions-shaped JSON with a `usage` field.
It delays responses by 25–100 ms, returns one HTTP 429 with `Retry-After: 1`,
then completes three classifications. A second run skips the checkpointed inputs
and makes no requests. The model name, endpoint, input-token estimates, and usage
values are placeholders for this demonstration.

## Runnable source

<!-- example: examples/llm_inference.py -->
```python
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
```
<!-- /example -->

The checkout provides `httpx` in its development dependencies. To adapt the source
outside the checkout, install `aqute==0.10.0` and `httpx`, and replace the endpoint,
model, credentials, and token estimates with your application's configuration.
The source imports `RateLimitedError` and `retry_after_seconds` from
`examples.http_client`; copy those definitions from the [HTTP source](http_client.md#runnable-source)
or supply equivalents. The `examples` package is not installed with Aqute.

## Requests and token budgets

RPM counts requests per minute; TPM counts tokens per minute. Limiting request
starts alone does not limit tokens when prompts and output lengths differ.
`workers_count=3` limits occupied workers independently of either quota.
This example controls estimated TPM; it does not add a separate RPM limiter.

`TokenCostLimiter` implements the public `RateLimiter.acquire(name, task)`
protocol in application code. Its cost callback reads `task.data` and reserves
`estimated_input_tokens + max_tokens` before every attempt, including retries.
The bucket starts full, refills continuously at 600 tokens per minute, and holds
at most 600 tokens. One request's positive estimated cost must fit that capacity;
an invalid cost raises `ValueError`. Through Aqute, it stops the run inside the
worker `ExceptionGroup`; see [limiter failures](usage.md#errors-retries-and-timeouts).
Initial bursts are possible: a full bucket can admit up to 1,200 estimated tokens
in its first minute (600 initially plus 600 refilled). This token bucket does not
enforce a strict rolling-minute ceiling.

This is an **upper-bound estimate without refund**, conditional on the input
estimate covering the actual input and `max_tokens` bounding the counted output.
The supplied estimates are illustrative, not a tokenizer. Provider accounting
differs by vendor and can include other token categories or reservation rules;
the local limiter is a budget model, not a mirror of provider accounting.
The response's `usage` is logged only. Failed attempts keep their reservation,
and a successful response does not return unused tokens to the bucket.

This conservative reservation underutilizes the budget when `max_tokens` greatly
exceeds typical output length. Set `max_tokens` to the task's required output
size, not an arbitrary safety margin. A refund based on reported usage could be
an application extension for free-form generation, but is not implemented here.

## Retries and throttling

`PausableRateLimiter` wraps the token limiter. On HTTP 429, the handler parses
`Retry-After`, extends the shared pause, and raises a retryable error.
Already admitted requests can still complete or return 429. Admissions delayed
by the pause can resume together; this wrapper does not guarantee spacing after
the pause. Header parsing and fallback behavior follow the [HTTP recipe](http_client.md#limits-and-outcomes).

`retry_count=2` allows at most three attempts per input. Only HTTP 429 and
`httpx.TransportError` are retried, with a 0.1-second per-worker delay in addition
to any remaining admission wait. Other HTTP errors propagate through
`task.unwrap()`, stop the batch, and leave earlier checkpoints intact.
The five-second HTTPX timeout applies to network operations, not the whole batch,
token-budget waits, or shared pauses.

There is no SDK retry layer in this example; the real HTTP transport explicitly
uses `retries=0`, and the mock transport performs no automatic retries.
If you replace HTTPX with an SDK, disable its internal retries (often
`max_retries=0`) and let Aqute retry. Otherwise one handler attempt can make
multiple requests after a single token reservation. Disable retries on one side
to avoid multiplication: `retry_count=0` avoids Aqute retries, but SDK-internal
attempts would still bypass this limiter. Disable SDK retries when the limiter
must see every attempt. Select retries with your provider's billing and duplicate
request behavior in mind; a transport failure does not prove inference never ran.

## Checkpoints and cleanup

After consuming a successful result, the application stores
`checkpoint[task.data] = output`. A new batch filters those inputs out before
submission. This demonstration keeps the checkpoint in memory, keyed by the full
`Prompt` value. It is lost on process exit and assumes unique inputs for a fixed
model and configuration. Duplicate inputs within one run can still be submitted.

For resumable application work, persist the output and completion key together
and include the model and relevant request configuration in that key. A crash
after inference but before persistence can repeat a request; this example does
not provide exactly-once execution or a durable queue. Application checkpoints
also retain data outside Aqute's queue bounds. Each batch creates a fresh limiter,
so restarting a batch resets the local budget and shared pause. Frequent short
batches can therefore exceed the intended aggregate budget; share a limiter
across batches in an application that needs continuous admission accounting.

The managed result stream waits for producer and worker cleanup before the HTTP
client closes. This requires cooperative cancellation. The example buffers each
JSON response in memory; finite queues bound item counts, not response bytes.
