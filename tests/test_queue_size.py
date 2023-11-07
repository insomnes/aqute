import asyncio
from time import perf_counter
from typing import List

import pytest

from aqute import Aqute


async def run_with_queue_size(
    workers_count: int,
    input_queue_size: int,
    tasks_to_add: int,
    sleep_time: float,
) -> List[float]:
    async def sleep_handler(i: int) -> int:
        await asyncio.sleep(sleep_time)
        return i

    aqute = Aqute(
        handle_coro=sleep_handler,
        workers_count=workers_count,
        input_task_queue_size=input_queue_size,
    )
    add_timings = []
    async with aqute:
        total_start = perf_counter()
        for i in range(tasks_to_add):
            start = perf_counter()
            await aqute.add_task(i)
            add_timings.append(perf_counter() - start)

        await aqute.wait_till_end()
        total_elapsed = perf_counter() - total_start

    should_take = tasks_to_add * sleep_time / workers_count
    assert should_take <= total_elapsed < should_take + 0.03

    return add_timings


@pytest.mark.asyncio
async def test_no_queue_size():
    add_timings = await run_with_queue_size(1, 0, 6, 0.05)

    # Since there is no queue size limit, all tasks are added instantly
    # because they do not have to wait for a slot to open up in the queue.
    assert all([t < 0.05 for t in add_timings])


@pytest.mark.asyncio
async def test_queue_size_one():
    add_timings = await run_with_queue_size(1, 1, 6, 0.05)

    # The first two tasks are added instantly
    # due to the available worker and one queue slot.
    # The remaining tasks have to wait for the worker to finish and free up the slot.
    expected = [True, True, False, False, False, False]
    assert [t < 0.05 for t in add_timings] == expected


@pytest.mark.asyncio
async def test_queue_size_two():
    add_timings = await run_with_queue_size(1, 2, 7, 0.05)

    # The first three tasks are added instantly
    # because of the two queue slots plus one worker.
    # Subsequent tasks have to wait for the worker to process a task
    # before they can be added.
    expected = [True, True, True, False, False, False, False]
    assert [t < 0.05 for t in add_timings] == expected


@pytest.mark.asyncio
async def test_two_workers():
    add_timings = await run_with_queue_size(2, 1, 9, 0.05)

    # The first three tasks are added instantly (two workers plus one queue slot).
    # As there are two workers, the tasks are processed faster,
    # but due to the queue size being one,
    # the task additions alternate between instant and waiting.
    expected = [True, True, True, False, True, False, True, False, True]
    assert [t < 0.05 for t in add_timings] == expected


@pytest.mark.asyncio
async def test_two_workers_size_two():
    add_timings = await run_with_queue_size(2, 2, 9, 0.05)

    # 1. The first four tasks are added instantly. This is because there are two workers
    #    and a queue size of two, which means that there's enough capacity
    #    to handle four tasks immediately (two being processed, two in queue).
    # 2. As tasks are being processed, the fifth task must wait for the first task
    #    to complete because there are only two workers and the queue is full.
    # 3. After the first task completes, the fifth task is added with 0.05 timing.
    # 4. Second task finishes almost same time, sixth task is added to the queue
    #    and we get instant adding time.
    # 5. Now seventh task waits for third task to finish, and worker to free the slot.
    # And so on...

    expected = [True, True, True, True, False, True, False, True, False]
    assert [t < 0.05 for t in add_timings] == expected


@pytest.mark.asyncio
async def test_three_workers_size_two():
    add_timings = await run_with_queue_size(3, 2, 12, 0.05)

    # 1. The first five tasks are added instantly.
    #    With three workers and a queue size of two, there's capacity for five tasks
    #    to be added immediately (three being processed, two in queue).
    # 2. As tasks are being processed, the sixth task must wait for the first
    #    to complete because all three workers are busy and the queue is full.
    # 3. After the first task completition, the sixth task is added with 0.05 timing.
    # 4. As the second and third tasks finish almost the same time,
    #    the seventh and eighth tasks are added instantly, because
    #    new slots in the queue are available now.
    # 5. This pattern continues with the ninth task waiting for the fourth task
    #    before it can be added, as all workers are occupied again.
    # 6. With each set of three tasks being processed,
    #    there is a brief period where the queue is full, resulting in
    #    every third task having to wait for the previous set to finish.
    # And so on...
    expected = [
        True,
        True,
        True,
        True,
        True,
        False,
        True,
        True,
        False,
        True,
        True,
        False,
    ]
    assert [t < 0.05 for t in add_timings] == expected
