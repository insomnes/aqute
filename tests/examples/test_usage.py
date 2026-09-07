import asyncio
import subprocess
import sys
from collections import Counter

import httpx
import pytest

from examples import http_client, service_shutdown, streaming


@pytest.mark.parametrize(
    ("module", "expected"),
    [
        ("quickstart", "[0, 2, 4, 6, 8, 10, 12, 14, 16, 18]"),
        ("streaming", "[0, 2, 4, 6, 8, 10, 12, 14]"),
        ("http_client", "['/jobs/1', '/jobs/2']"),
    ],
)
def test_example_entrypoint(module, expected):
    result = subprocess.run(
        [sys.executable, "-m", f"examples.{module}"],
        capture_output=True,
        text=True,
        check=True,
        timeout=5,
    )
    assert f"Results: {expected}" in result.stderr


@pytest.mark.asyncio
async def test_streaming_example():
    assert sorted(await streaming.main()) == [value * 2 for value in range(8)]


@pytest.mark.asyncio
async def test_shared_client_example():
    assert sorted(await http_client.main()) == ["/jobs/1", "/jobs/2"]


@pytest.mark.asyncio
async def test_http_example_retries_transport_failure():
    calls = Counter()

    def respond(request: httpx.Request) -> httpx.Response:
        calls[request.url.path] += 1
        if calls[request.url.path] == 1:
            raise httpx.ConnectError("connection refused", request=request)
        return httpx.Response(200, text=request.url.path)

    pages = await http_client.fetch_pages(
        ["https://example.test/jobs/1"], transport=httpx.MockTransport(respond)
    )
    assert pages == ["/jobs/1"]
    assert calls == {"/jobs/1": 2}


@pytest.mark.asyncio
async def test_http_example_propagates_http_failure():
    def respond(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(503)

    with pytest.raises(httpx.HTTPStatusError):
        await http_client.fetch_pages(
            ["https://example.test/jobs/1"], transport=httpx.MockTransport(respond)
        )


@pytest.mark.asyncio
async def test_service_example_drains_before_shutdown():
    values = await service_shutdown.main()
    assert len(values) >= 3
    assert sorted(values) == list(range(len(values)))


@pytest.mark.asyncio
async def test_service_example_deadline_cancels_active_work():
    started = []
    cleaned = []

    async def handle(job_id: int) -> int:
        started.append(job_id)
        try:
            await asyncio.Event().wait()
            return job_id
        finally:
            cleaned.append(job_id)

    async with asyncio.timeout(1):
        values, timed_out = await service_shutdown.serve(handle, drain_timeout=0.01)
    assert timed_out
    assert values == []
    assert len(started) == 2
    assert sorted(cleaned) == sorted(started)
