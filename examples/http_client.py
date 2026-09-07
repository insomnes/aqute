"""Use one shared HTTPX client; main() runs an offline transport demonstration."""

import asyncio
import logging
from collections.abc import AsyncIterable, Iterable
from contextlib import aclosing

import httpx

from aqute import Aqute
from examples.streaming import retry_delay


async def fetch_pages(
    urls: Iterable[str] | AsyncIterable[str],
    *,
    transport: httpx.AsyncBaseTransport | None = None,
) -> list[str]:
    async with httpx.AsyncClient(timeout=5.0, transport=transport) as client:

        async def fetch(url: str) -> str:
            response = await client.get(url)
            response.raise_for_status()
            return response.text

        engine = Aqute(
            fetch,
            4,
            input_task_queue_size=8,
            result_queue=asyncio.Queue(8),
            retry_count=2,
            retry_delay=retry_delay,
            specific_errors_to_retry=httpx.TransportError,
        )
        pages = []
        async with aclosing(engine.iter_results(urls)) as results:
            async for task in results:
                if task.error is not None:
                    raise task.error
                assert task.result is not None
                pages.append(task.result)
        return pages


async def main() -> list[str]:
    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=request.url.path)

    pages = await fetch_pages(
        ["https://example.test/jobs/1", "https://example.test/jobs/2"],
        transport=httpx.MockTransport(respond),
    )
    assert sorted(pages) == ["/jobs/1", "/jobs/2"]
    return pages


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("Results: %s", sorted(asyncio.run(main())))
