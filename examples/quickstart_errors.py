"""Report each failed item and keep the successful results."""

import asyncio
import logging

from aqute import Aqute


async def parse_port(value: str) -> int:
    await asyncio.sleep(0.1)  # Simulate asynchronous I/O.
    return int(value)


async def main() -> None:
    tasks = await Aqute(parse_port, workers_count=2).process_all(
        ["443", "invalid", "8080"]
    )
    for task in tasks:
        if task.error is not None:
            logging.warning("Failed %r: %s", task.data, task.error)
        else:
            logging.info("Port: %s", task.unwrap())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
