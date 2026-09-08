from email.utils import formatdate
from time import time

import httpx
import pytest

from examples.http_client import fetch_pages, retry_after_seconds


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("0", 0),
        ("2", 2),
        ("0.5", 0.5),
        (None, 1),
        ("", 1),
        ("junk", 1),
        ("-1", 1),
        ("nan", 1),
        ("inf", 1),
        ("1e309", 1),
    ],
)
def test_retry_after_seconds_and_invalid_fallback(value, expected):
    """The HTTP recipe returns usable seconds or its documented fallback."""
    assert retry_after_seconds(value) == expected


def test_retry_after_http_dates():
    """HTTP dates convert from wall time; expired dates add no pause."""
    now = time()
    future = formatdate(now + 60, usegmt=True)
    assert 58 < retry_after_seconds(future) <= 60
    assert retry_after_seconds(formatdate(now - 60, usegmt=True)) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("retry_after", ["0", formatdate(0, usegmt=True)])
async def test_http_example_retries_429(retry_after):
    """A 429 can recover through the public recipe's normal retry path."""
    calls = 0

    def respond(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(429, headers={"Retry-After": retry_after})
        return httpx.Response(200, text="recovered")

    completed = await fetch_pages(
        ["https://example.test/jobs/1"], transport=httpx.MockTransport(respond)
    )
    assert completed == 1
    assert calls == 2


@pytest.mark.asyncio
async def test_http_example_exhausts_429_retries():
    """A persistent 429 remains a terminal HTTP status error after three calls."""
    calls = 0

    def respond(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(429, headers={"Retry-After": "0"})

    with pytest.raises(httpx.HTTPStatusError) as error:
        await fetch_pages(
            ["https://example.test/jobs/1"], transport=httpx.MockTransport(respond)
        )
    assert error.value.response.status_code == 429
    assert calls == 3
