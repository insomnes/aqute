"""Submit custom task IDs and priorities, then drain results after completion."""

import asyncio
import logging

from aqute import Aqute


async def handle(value: int) -> int:
    return value * 2


async def main() -> list[tuple[str, int]]:
    # Use process_all() when custom task IDs and priorities are unnecessary.
    engine = Aqute(
        handle,
        workers_count=2,
        use_priority_queue=True,
        # Results must be unlimited because consumption starts after finish().
        result_queue=asyncio.Queue(0),
    )
    async with engine:
        for value in range(10):
            await engine.add_task(
                value, task_id=f"job-{value}", task_priority=10 - value
            )
        await engine.finish()

    # Priority applies to pending tasks; it does not preempt active workers.
    return sorted((task.task_id, task.unwrap()) for task in engine.drain_results())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("Results: %s", asyncio.run(main()))
