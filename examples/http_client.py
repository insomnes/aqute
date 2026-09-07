"""Use one shared HTTPX client; main() runs an offline transport demonstration."""

import asyncio
import logging
from collections.abc import AsyncIterable, Iterable

import httpx

from aqute import Aqute
from aqute.ratelimiter import TokenBucketRateLimiter
from examples.streaming import retry_delay

logger = logging.getLogger(__name__)


async def fetch_pages(
    urls: Iterable[str] | AsyncIterable[str],
    *,
    transport: httpx.AsyncBaseTransport | None = None,
) -> int:
    """Log pages as they complete; raise the first observed terminal error."""
    async with httpx.AsyncClient(timeout=5.0, transport=transport) as client:

        async def fetch(url: str) -> str:
            response = await client.get(url)
            response.raise_for_status()
            return response.text

        engine = Aqute(
            fetch,
            workers_count=4,
            rate_limiter=TokenBucketRateLimiter(max_rate=10),
            retry_count=2,
            retry_delay=retry_delay,
            specific_errors_to_retry=httpx.TransportError,
        )
        completed = 0
        async with engine.iter_results(urls) as results:
            async for task in results:
                # Replace this log with application-owned parsing or persistence.
                logger.info("Fetched %s: %s", task.data, task.unwrap())
                completed += 1
        return completed


async def main() -> int:
    failed_once = False

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal failed_once
        if request.url.path == "/jobs/1" and not failed_once:
            failed_once = True
            raise httpx.ConnectError("temporary connection failure", request=request)
        if request.url.path == "/unavailable":
            return httpx.Response(503)
        return httpx.Response(200, text=request.url.path)

    completed = await fetch_pages(
        ("https://example.test/jobs/1", "https://example.test/jobs/2"),
        transport=httpx.MockTransport(respond),
    )
    try:
        await fetch_pages(
            ("https://example.test/unavailable",),
            transport=httpx.MockTransport(respond),
        )
    except httpx.HTTPStatusError as error:
        logger.error(
            "Terminal HTTP %s for %s; run stopped",
            error.response.status_code,
            error.request.url,
        )
    return completed


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("Results: %s pages", asyncio.run(main()))
