"""Consume bounded streaming results and stop early with managed cleanup."""

import asyncio
import logging
from random import uniform

from aqute import Aqute

logger = logging.getLogger(__name__)
RESULT_LIMIT = 3


async def main() -> int:
    async def source():
        try:
            for value in range(100):
                yield value
        finally:
            logger.info("Source closed")

    async def handle(value: int) -> int:
        await asyncio.sleep(uniform(0.025, 0.1))  # Simulate asynchronous I/O.
        return value * 2

    engine = Aqute(handle, workers_count=3)
    completed = 0
    async with engine.iter_results(source()) as results:
        async for task in results:
            # Replace this log with application-owned processing or persistence.
            logger.info("Result for %s: %s", task.data, task.unwrap())
            completed += 1
            if completed == RESULT_LIMIT:
                break
    # Context exit has awaited producer and worker cleanup.
    return completed


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("Results: %s consumed", asyncio.run(main()))
