"""Bounded asynchronous input, retries, and progress counters."""

import asyncio
import logging
from collections import Counter
from random import uniform

from aqute import Aqute

logger = logging.getLogger(__name__)
FAIL_ONCE = 3


def retry_delay(failed_attempt: int, _error: Exception) -> float:
    ceiling = min(5.0, 0.01 * 2 ** min(failed_attempt - 1, 10))
    return uniform(0.0, ceiling)


async def main() -> list[int]:
    attempts: Counter[int] = Counter()

    async def source():
        for value in range(8):
            yield value

    async def handle(value: int) -> int:
        attempts[value] += 1
        if value == FAIL_ONCE and attempts[value] == 1:
            raise ValueError("transient example failure")
        return value * 2

    engine = Aqute(
        handle,
        3,
        input_task_queue_size=4,
        result_queue=asyncio.Queue(4),
        retry_count=2,
        retry_delay=retry_delay,
        specific_errors_to_retry=ValueError,
    )
    values = []
    async with engine.iter_results(source()) as results:
        async for task in results:
            if task.error is not None:
                raise task.error
            assert task.result is not None
            values.append(task.result)
            counts = engine.counters
            logger.info(
                "Pending=%s running=%s succeeded=%s failed=%s retries=%s",
                counts.pending,
                counts.running,
                counts.succeeded,
                counts.failed,
                counts.retries,
            )
    assert sorted(values) == [value * 2 for value in range(8)]
    return values


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("Results: %s", sorted(asyncio.run(main())))
