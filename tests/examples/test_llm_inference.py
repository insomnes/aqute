import asyncio
import json
from time import monotonic

import httpx
import pytest

from aqute import Aqute, AquteTask
from examples.llm_inference import Prompt, TokenCostLimiter, infer_batch


@pytest.mark.asyncio
async def test_inference_retries_429_and_resumes_from_checkpoint():
    """Throttle once, save the output, and make no request for a saved input."""
    prompt = Prompt("Classify: service is healthy", 10, 8)
    checkpoint = {}
    requests = []
    arrivals = []

    def respond(request):
        requests.append(json.loads(request.content))
        arrivals.append(monotonic())
        if len(requests) == 1:
            return httpx.Response(429, headers={"Retry-After": "1"})
        return httpx.Response(
            200,
            json={
                "choices": [{"message": {"content": "ok"}}],
                "usage": {"total_tokens": 9},
            },
        )

    assert (
        await infer_batch([prompt], checkpoint, transport=httpx.MockTransport(respond))
        == 1
    )
    assert checkpoint == {prompt: "ok"}
    assert len(requests) == 2
    assert (
        requests[0]
        == requests[1]
        == {
            "model": "your-model-name",
            "messages": [{"role": "user", "content": prompt.text}],
            "max_tokens": 8,
        }
    )
    assert arrivals[1] - arrivals[0] >= 1

    assert (
        await infer_batch([prompt], checkpoint, transport=httpx.MockTransport(respond))
        == 0
    )
    assert len(requests) == 2


@pytest.mark.asyncio
async def test_terminal_http_error_preserves_checkpoint_without_retry():
    """A terminal failure remains unsaved and does not discard earlier output."""
    completed = Prompt("Saved input", 10, 8)
    failed = Prompt("Failing input", 10, 8)
    checkpoint = {completed: "ok"}
    calls = []

    def respond(request):
        calls.append(json.loads(request.content)["messages"][0]["content"])
        return httpx.Response(503)

    with pytest.raises(httpx.HTTPStatusError):
        await infer_batch(
            [completed, failed], checkpoint, transport=httpx.MockTransport(respond)
        )
    assert checkpoint == {completed: "ok"}
    assert calls == [failed.text]


@pytest.mark.asyncio
async def test_token_cost_controls_admission():
    """After spending the full budget, 25 more tokens require 0.25s of refill."""
    limiter = TokenCostLimiter(6000, lambda prompt: prompt.estimated_input_tokens)
    started = monotonic()
    await limiter.acquire(task=AquteTask(Prompt("large", 6000, 0), "large"))
    await limiter.acquire(task=AquteTask(Prompt("small", 25, 0), "small"))
    assert monotonic() - started >= 0.25


@pytest.mark.asyncio
async def test_failed_attempt_keeps_its_token_reservation():
    """A retry must wait for more budget even when the previous attempt failed."""
    calls = []

    async def fail(prompt):
        calls.append(prompt)
        raise ConnectionError("temporary failure")

    prompt = Prompt("large", 5900, 100)
    limiter = TokenCostLimiter(
        6000, lambda prompt: prompt.estimated_input_tokens + prompt.max_tokens
    )
    engine = Aqute(fail, workers_count=1, rate_limiter=limiter, retry_count=1)
    async with asyncio.timeout(2):
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.1):
                await engine.process_all([prompt])
    assert calls == [prompt]


@pytest.mark.asyncio
@pytest.mark.parametrize("cost", [0, -1, 61])
async def test_invalid_cost_is_rejected_without_waiting(cost):
    """An impossible reservation raises instead of waiting indefinitely."""
    limiter = TokenCostLimiter(60, lambda prompt: prompt.estimated_input_tokens)
    async with asyncio.timeout(1):
        with pytest.raises(ValueError, match="cost"):
            await limiter.acquire(task=AquteTask(Prompt("invalid", cost, 0), "bad"))
