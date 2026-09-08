import asyncio

import pytest

from aqute import Aqute, AquteError


async def run_with_queue_size(
    workers_count: int,
    input_queue_size: int | None,
    tasks_to_add: int,
    use_priority_queue: bool = False,
) -> None:
    started: asyncio.Queue[int] = asyncio.Queue()
    release = [asyncio.Event() for _ in range(tasks_to_add)]

    async def controlled_handler(i: int) -> int:
        started.put_nowait(i)
        await release[i].wait()
        return i * 2

    aqute = Aqute(
        handle_coro=controlled_handler,
        workers_count=workers_count,
        input_task_queue_size=input_queue_size,
        use_priority_queue=use_priority_queue,
        result_queue=asyncio.Queue(0),
    )

    capacity = workers_count if input_queue_size is None else input_queue_size

    async def submit(i: int, submitting: asyncio.Event) -> str:
        submitting.set()
        task_id = await aqute.add_task(i)
        assert aqute.counters.pending <= capacity
        return task_id

    # This watchdog detects stalled progress; elapsed time is not an oracle.
    async with asyncio.timeout(5), aqute, asyncio.TaskGroup() as submissions:
        try:
            active = []
            for i in range(workers_count):
                await aqute.add_task(i)
                active.append(await started.get())

            if capacity == 0:
                # Every remaining input is admitted while all workers are held.
                for i in range(workers_count, tasks_to_add):
                    await aqute.add_task(i)
                assert aqute.counters.pending == tasks_to_add - workers_count
            else:
                occupied = workers_count + capacity
                for i in range(workers_count, occupied):
                    await aqute.add_task(i)
                assert aqute.counters.pending == capacity

                for i in range(occupied, tasks_to_add):
                    submitting = asyncio.Event()
                    submission = submissions.create_task(submit(i, submitting))
                    await submitting.wait()
                    assert not submission.done()

                    # Release one actual handler, without assuming worker order.
                    release[active.pop(0)].set()
                    active.append(await started.get())
                    await submission
                    assert aqute.counters.pending == capacity

            for gate in release:
                gate.set()
            await aqute.finish()
            results = aqute.drain_results()
            assert len(results) == tasks_to_add
            assert all(task.success and task.error is None for task in results)
            assert sorted((task.data, task.result) for task in results) == [
                (i, i * 2) for i in range(tasks_to_add)
            ]
        finally:
            for gate in release:
                gate.set()


@pytest.mark.asyncio
async def test_unlimited_input_queue():
    await run_with_queue_size(1, 0, 6)


@pytest.mark.asyncio
@pytest.mark.parametrize("use_priority_queue", [False, True], ids=["normal", "prior"])
async def test_queue_size_one(use_priority_queue: bool):
    await run_with_queue_size(1, 1, 6, use_priority_queue)


@pytest.mark.asyncio
async def test_queue_size_two():
    await run_with_queue_size(1, 2, 7)


@pytest.mark.asyncio
async def test_two_workers():
    await run_with_queue_size(2, 1, 9)


@pytest.mark.asyncio
async def test_two_workers_size_two():
    await run_with_queue_size(2, 2, 9)


@pytest.mark.asyncio
async def test_three_workers_size_two():
    await run_with_queue_size(3, 2, 12)


@pytest.mark.asyncio
@pytest.mark.parametrize("workers_count", [1, 2])
async def test_default_input_capacity(workers_count):
    """Omitted input capacity blocks admission at workers_count pending items."""
    await run_with_queue_size(workers_count, None, 10)


@pytest.mark.asyncio
@pytest.mark.parametrize("capacity", [None, 3])
@pytest.mark.parametrize("use_priority_queue", [False, True])
async def test_full_input_before_start_rejects_without_admitting(
    capacity, use_priority_queue
):
    """Pre-start overflow raises promptly and leaves accepted work intact."""

    async def double(value: int) -> int:
        return value * 2

    engine = Aqute(
        double, 2, input_task_queue_size=capacity, use_priority_queue=use_priority_queue
    )
    limit = 2 if capacity is None else capacity
    async with asyncio.timeout(1):
        for value in range(limit):
            assert await engine.add_task(value) == str(value)
        with pytest.raises(AquteError, match=r"start\(\).*input_task_queue_size"):
            await engine.add_task(99)
        assert engine.counters.pending == limit
        async with engine:
            results = [await engine.get_result() for _ in range(limit)]
            assert await engine.add_task(limit) == str(limit)
            results.append(await engine.get_result())
            await engine.finish()
    assert sorted((task.data, task.unwrap()) for task in results) == [
        (value, value * 2) for value in range(limit + 1)
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("pre_submit", [False, True])
async def test_manual_run_then_drain_with_explicit_unlimited_queues(pre_submit):
    """Documented manual settings finish all work before result consumption."""

    async def double(value: int) -> int:
        return value * 2

    queue = asyncio.Queue(0)
    engine = Aqute(
        double,
        4,
        input_task_queue_size=0 if pre_submit else None,
        result_queue=queue,
    )
    async with asyncio.timeout(2):
        try:
            if not pre_submit:
                engine.start()
            for value in range(100):
                await engine.add_task(value, task_id=f"job-{value}")
            if pre_submit:
                await engine.run()
            else:
                await engine.finish()
        finally:
            await engine.stop()
    assert engine.result_queue is queue
    assert sorted(
        (task.data, task.task_id, task.unwrap()) for task in engine.drain_results()
    ) == [(value, f"job-{value}", value * 2) for value in range(100)]


@pytest.mark.asyncio
async def test_default_manual_queues_with_concurrent_consumer():
    """Default manual queues finish a large batch when results drain concurrently."""

    async def double(value: int) -> int:
        return value * 2

    engine = Aqute(double, 4)
    assert engine.result_queue.maxsize == 4

    async def consume():
        return [await engine.get_result() for _ in range(100)]

    async with asyncio.timeout(2), engine, asyncio.TaskGroup() as group:
        consumer = group.create_task(consume())
        for value in range(100):
            await engine.add_task(value)
        await engine.finish()
    assert sorted(task.unwrap() for task in consumer.result()) == [
        value * 2 for value in range(100)
    ]
