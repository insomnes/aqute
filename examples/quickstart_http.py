"""Fetch URLs with shared connections, rate limits, and transport retries."""

import asyncio
import logging

import httpx

from aqute import Aqute
from aqute.ratelimiter import TokenBucketRateLimiter


async def main() -> None:
    urls = ["https://example.com/", "https://example.org/"]
    async with httpx.AsyncClient(timeout=5.0) as client:

        async def fetch(url: str) -> int:
            response = await client.get(url)
            response.raise_for_status()
            return response.status_code

        engine = Aqute(
            fetch,
            workers_count=4,
            rate_limiter=TokenBucketRateLimiter(max_rate=5),
            retry_count=2,
            retry_delay=lambda _attempt, _error: 0.5,
            specific_errors_to_retry=httpx.TransportError,
        )
        async with engine.iter_results(urls) as results:
            async for task in results:
                if task.error is not None:
                    logging.warning("Failed %s: %s", task.data, task.error)
                else:
                    logging.info("%s: HTTP %s", task.data, task.unwrap())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
