import asyncio
import logging
import subprocess
import sys
from collections import Counter

import httpx
import pytest

from examples import http_client, retry_progress, service_shutdown, streaming


@pytest.mark.parametrize(
    ("module", "expected"),
    [
        ("quickstart", "[0, 2, 4, 6, 8, 10, 12, 14, 16, 18]"),
        ("streaming", "3 consumed"),
        ("retry_progress", "[0, 2, 4, 6, 8, 10, 12, 14]"),
        ("http_client", "2 pages"),
        ("llm_inference", "3 completions"),
        ("manual_drain", str([(f"job-{value}", value * 2) for value in range(10)])),
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
async def test_streaming_example(caplog):
    """The example must emit three doubled values and close its source on exit."""
    caplog.set_level(logging.INFO, logger="examples.streaming")
    assert await streaming.main() == 3
    messages = [
        message
        for name, _level, message in caplog.record_tuples
        if name == "examples.streaming"
    ]
    assert messages.count("Source closed") == 1
    results = [message for message in messages if message != "Source closed"]
    assert len(results) == len(set(results)) == 3
    assert set(results) <= {f"Result for {value}: {value * 2}" for value in range(100)}


@pytest.mark.asyncio
async def test_retry_progress_example():
    """The advanced recipe must still return all eight values after retrying."""
    assert sorted(await retry_progress.main()) == [value * 2 for value in range(8)]


@pytest.mark.asyncio
async def test_shared_client_example(caplog):
    caplog.set_level(logging.INFO, logger="examples.http_client")
    assert await http_client.main() == 2
    messages = [
        message
        for name, _level, message in caplog.record_tuples
        if name == "examples.http_client"
    ]
    assert sorted(messages) == [
        "Fetched https://example.test/jobs/1: /jobs/1",
        "Fetched https://example.test/jobs/2: /jobs/2",
        "Terminal HTTP 503 for https://example.test/unavailable; run stopped",
    ]


@pytest.mark.asyncio
async def test_http_example_retries_transport_failure(caplog):
    caplog.set_level(logging.INFO, logger="examples.http_client")
    calls = Counter()

    def respond(request: httpx.Request) -> httpx.Response:
        calls[request.url.path] += 1
        if calls[request.url.path] == 1:
            raise httpx.ConnectError("connection refused", request=request)
        return httpx.Response(200, text=request.url.path)

    completed = await http_client.fetch_pages(
        ["https://example.test/jobs/1"], transport=httpx.MockTransport(respond)
    )
    assert completed == 1
    messages = [
        message
        for name, _level, message in caplog.record_tuples
        if name == "examples.http_client"
    ]
    assert messages == ["Fetched https://example.test/jobs/1: /jobs/1"]
    assert calls == {"/jobs/1": 2}


@pytest.mark.asyncio
async def test_http_example_propagates_http_failure():
    calls = 0

    def respond(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503)

    with pytest.raises(httpx.HTTPStatusError):
        await http_client.fetch_pages(
            ["https://example.test/jobs/1"], transport=httpx.MockTransport(respond)
        )
    assert calls == 1


class ClosingTransport(httpx.MockTransport):
    closed = False

    async def aclose(self):
        await super().aclose()
        self.closed = True


@pytest.mark.asyncio
async def test_http_failure_closes_started_source_and_client():
    source_closed = False

    async def source():
        nonlocal source_closed
        try:
            yield "https://example.test/unavailable"
            await asyncio.Event().wait()
        finally:
            source_closed = True

    transport = ClosingTransport(lambda _request: httpx.Response(503))
    async with asyncio.timeout(1):
        with pytest.raises(httpx.HTTPStatusError):
            await http_client.fetch_pages(source(), transport=transport)
    assert source_closed
    assert transport.closed


@pytest.mark.asyncio
async def test_http_source_failure_cancels_request_and_closes_client():
    request_started = asyncio.Event()
    request_closed = False
    source_closed = False

    async def respond(_request: httpx.Request) -> httpx.Response:
        nonlocal request_closed
        request_started.set()
        try:
            await asyncio.Event().wait()
            return httpx.Response(200)
        finally:
            request_closed = True

    async def source():
        nonlocal source_closed
        try:
            yield "https://example.test/jobs/1"
            await request_started.wait()
            raise ValueError("input unavailable")
        finally:
            source_closed = True

    transport = ClosingTransport(respond)
    async with asyncio.timeout(1):
        with pytest.raises(ValueError, match="input unavailable"):
            await http_client.fetch_pages(source(), transport=transport)
    assert request_closed
    assert source_closed
    assert transport.closed


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
