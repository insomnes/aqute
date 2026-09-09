"""Run a finite batch and receive results in input order."""

import asyncio
import logging
from random import uniform

from aqute import Aqute


async def handle(value: int) -> int:
    await asyncio.sleep(uniform(0.025, 0.1))  # Simulate asynchronous I/O.
    return value * 2


async def main() -> list[int]:
    tasks = await Aqute(handle, workers_count=4).process_all(range(10))
    values = [task.unwrap() for task in tasks]
    assert values == [value * 2 for value in range(10)]
    return values


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("Results: %s", asyncio.run(main()))
