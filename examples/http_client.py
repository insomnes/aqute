"""Use one shared HTTPX client; main() runs an offline transport demonstration."""

import asyncio
import logging
import math
from collections.abc import AsyncIterable, Iterable
from email.utils import parsedate_to_datetime
from functools import partial
from time import monotonic, time

import httpx

from aqute import Aqute
from aqute.ratelimiter import PausableRateLimiter, TokenBucketRateLimiter
from examples.retry_progress import retry_delay

logger = logging.getLogger(__name__)
TOO_MANY_REQUESTS = 429


class RateLimitedError(httpx.HTTPStatusError):
    """Select HTTP 429 for retries without retrying other HTTP status errors."""


def retry_after_seconds(value: str | None) -> float:
    """Accept nonnegative numeric seconds or an HTTP-date; default to one second.

    Missing, malformed, negative, or nonfinite values use the fallback. Past
    HTTP dates return zero. HTTP-date conversion uses wall time only here;
    PausableRateLimiter stores the resulting delay on the monotonic clock.
    """
    if value is None:
        return 1.0
    try:
        seconds = float(value)
    except ValueError:
        try:
            deadline = parsedate_to_datetime(value)
            if deadline.tzinfo is None:
                return 1.0
            return max(0.0, deadline.timestamp() - time())
        except (TypeError, ValueError, OverflowError):
            return 1.0
    return seconds if math.isfinite(seconds) and seconds >= 0 else 1.0


def http_retry_delay(failed_attempt: int, error: Exception) -> float:
    """Keep per-worker retry delay for the comparison and transport backoff."""
    if isinstance(error, RateLimitedError):
        return retry_after_seconds(error.response.headers.get("Retry-After"))
    return retry_delay(failed_attempt, error)


async def fetch_page(
    client: httpx.AsyncClient, url: str, limiter: PausableRateLimiter | None
) -> str:
    """Pause shared admission on 429; None demonstrates only per-worker delay."""
    response = await client.get(url)
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as error:
        if response.status_code != TOO_MANY_REQUESTS:
            raise
        if limiter is not None:
            limiter.pause_for(retry_after_seconds(response.headers.get("Retry-After")))
        raise RateLimitedError(
            str(error), request=error.request, response=error.response
        ) from error
    return response.text


async def fetch_pages(
    urls: Iterable[str] | AsyncIterable[str],
    *,
    transport: httpx.AsyncBaseTransport | None = None,
) -> int:
    """Log pages as they complete; raise the first observed terminal error."""
    limiter = PausableRateLimiter(TokenBucketRateLimiter(max_rate=10))
    async with httpx.AsyncClient(timeout=5.0, transport=transport) as client:

        async def fetch(url: str) -> str:
            return await fetch_page(client, url, limiter)

        engine = Aqute(
            fetch,
            workers_count=4,
            rate_limiter=limiter,
            retry_count=2,
            retry_delay=http_retry_delay,
            specific_errors_to_retry=(httpx.TransportError, RateLimitedError),
        )
        completed = 0
        async with engine.iter_results(urls) as results:
            async for task in results:
                # Replace this log with application-owned parsing or persistence.
                logger.info("Fetched %s: %s", task.data, task.unwrap())
                completed += 1
        return completed


async def compare_throttling() -> None:
    """Compare eight workers against one 300 ms throttle window, offline.

    The transport responds immediately, before the next paced admission. Real
    requests already in flight can produce more 429s after a pause is set.
    These request counts do not predict throughput for a real service.
    """
    workers = 8
    urls = [f"https://example.test/jobs/{value}" for value in range(40)]
    for shared_pause in (False, True):
        throttled = 0
        window_end = 0.0

        def respond(request: httpx.Request) -> httpx.Response:
            nonlocal throttled, window_end
            if window_end == 0:
                window_end = monotonic() + 0.3
            if monotonic() < window_end:
                throttled += 1
                return httpx.Response(429, headers={"Retry-After": "1"})
            return httpx.Response(200, text=request.url.path)

        inner = TokenBucketRateLimiter(max_rate=100)
        limiter = PausableRateLimiter(inner) if shared_pause else None
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            engine = Aqute(
                partial(fetch_page, client, limiter=limiter),
                workers_count=workers,
                rate_limiter=limiter if limiter is not None else inner,
                retry_count=2,
                retry_delay=http_retry_delay,
                specific_errors_to_retry=(httpx.TransportError, RateLimitedError),
            )
            started = monotonic()
            completed = retries = 0
            async with engine.iter_results(urls) as results:
                async for task in results:
                    task.unwrap()
                    completed += 1
                    retries = engine.counters.retries
            logger.info(
                "Throttle comparison: shared_pause=%s workers=%s pages=%s "
                "throttled_requests=%s retries=%s elapsed=%.3fs",
                shared_pause,
                workers,
                completed,
                throttled,
                retries,
                monotonic() - started,
            )


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
    asyncio.run(compare_throttling())
    logging.info("Results: %s pages", asyncio.run(main()))
