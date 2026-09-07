import asyncio

import pytest

from aqute import Aqute


async def run_with_queue_size(
    workers_count: int,
    input_queue_size: int,
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
    )

    async def submit(i: int, submitting: asyncio.Event) -> str:
        submitting.set()
        task_id = await aqute.add_task(i)
        assert aqute.counters.pending <= input_queue_size
        return task_id

    # This watchdog detects stalled progress; elapsed time is not an oracle.
    async with asyncio.timeout(5), aqute, asyncio.TaskGroup() as submissions:
        try:
            active = []
            for i in range(workers_count):
                await aqute.add_task(i)
                active.append(await started.get())

            if input_queue_size == 0:
                # Every remaining input is admitted while all workers are held.
                for i in range(workers_count, tasks_to_add):
                    await aqute.add_task(i)
                assert aqute.counters.pending == tasks_to_add - workers_count
            else:
                capacity = workers_count + input_queue_size
                for i in range(workers_count, capacity):
                    await aqute.add_task(i)
                assert aqute.counters.pending == input_queue_size

                for i in range(capacity, tasks_to_add):
                    submitting = asyncio.Event()
                    submission = submissions.create_task(submit(i, submitting))
                    await submitting.wait()
                    assert not submission.done()

                    # Release one actual handler, without assuming worker order.
                    release[active.pop(0)].set()
                    active.append(await started.get())
                    await submission
                    assert aqute.counters.pending == input_queue_size

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
async def test_no_queue_size():
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
