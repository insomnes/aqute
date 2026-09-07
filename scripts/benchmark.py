"""Measure Aqute scheduling with bounded queues and verified results.

Run from the repository root with PYTHONPATH=. uv run --locked python
scripts/benchmark.py. Use an isolated interpreter and PYTHONPATH pointing to a
source snapshot when comparing revisions. Imports and warmup are not timed.
"""

import argparse
import asyncio
import gc
import json
import platform
import sys
import time
from contextlib import aclosing

import aqute
from aqute import Aqute


class Work:
    def __init__(self, count: int, workers: int, delay: float | None):
        self.count = count
        self.workers = workers
        self.delay = delay
        # Allocate the correctness audit before starting the measurement.
        self.seen = bytearray(count)
        self.calls = self.returned = self.active = self.peak_active = 0
        self.produced = self.peak_ahead = 0

    def source(self):
        for value in range(self.count):
            self.produced += 1
            self.peak_ahead = max(self.peak_ahead, self.produced - self.returned)
            yield value

    async def handle(self, value: int) -> int:
        self.calls += 1
        self.active += 1
        self.peak_active = max(self.peak_active, self.active)
        try:
            if self.delay is not None:
                await asyncio.sleep(self.delay)
            return value
        finally:
            self.active -= 1

    def consume(self, task):
        assert task.success and task.error is None
        value = task.result
        assert isinstance(value, int) and 0 <= value < self.count
        assert task.data == value and not self.seen[value]
        self.seen[value] = 1
        self.returned += 1

    def verify(self):
        assert self.calls == self.returned == self.count and all(self.seen)
        assert self.active == 0 and self.peak_active <= self.workers
        assert self.peak_ahead <= 4 * self.workers + 3

    async def run(self, mode: str):
        engine = Aqute(
            self.handle,
            self.workers,
            input_task_queue_size=self.workers,
            result_queue=asyncio.Queue(self.workers),
        )
        if mode == "helper":
            async with aclosing(engine.iter_results(self.source())) as results:
                async for task in results:
                    self.consume(task)
        else:
            async with engine:

                async def produce():
                    for value in self.source():
                        await engine.add_task(value)
                    engine.finish_submitting()

                async def consume():
                    for _ in range(self.count):
                        self.consume(await engine.get_result())

                async with asyncio.TaskGroup() as group:
                    group.create_task(produce())
                    group.create_task(consume())
                await engine.finish()


async def sample(args):
    delay = {"immediate": None, "yield": 0, "sleep1": 0.001, "sleep10": 0.01}[args.case]
    warm = Work(min(args.count, 128), args.workers, delay)
    await warm.run(args.mode)
    warm.verify()
    del warm
    gc.collect()
    work = Work(args.count, args.workers, delay)
    cpu, wall = time.process_time(), time.perf_counter()
    await work.run(args.mode)
    seconds, cpu_seconds = time.perf_counter() - wall, time.process_time() - cpu
    work.verify()
    return {
        "python": platform.python_version(),
        "source": aqute.__file__,
        "mode": args.mode,
        "case": args.case,
        "workers": args.workers,
        "items": args.count,
        "seconds": seconds,
        "cpu_seconds": cpu_seconds,
        "items_per_second": args.count / seconds,
        "cpu_us_per_item": cpu_seconds * 1e6 / args.count,
        "peak_active": work.peak_active,
        "peak_generated_ahead": work.peak_ahead,
        "results": work.returned,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["helper", "manual"], default="helper")
    parser.add_argument(
        "--case",
        choices=["immediate", "yield", "sleep1", "sleep10"],
        default="immediate",
    )
    parser.add_argument("--workers", type=int, default=32)
    parser.add_argument("--count", type=int, default=50000)
    args = parser.parse_args()
    if args.count < 1 or args.workers < 1:
        parser.error("--count and --workers must be positive")
    sys.stdout.write(json.dumps(asyncio.run(sample(args))) + "\n")


if __name__ == "__main__":
    main()
