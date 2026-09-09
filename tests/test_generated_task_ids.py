import asyncio

import pytest

from aqute import Aqute


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_blocked", [False, True])
@pytest.mark.parametrize("use_priority_queue", [False, True])
async def test_concurrent_generated_ids_match_completed_inputs(
    cancel_blocked: bool, use_priority_queue: bool
):
    """Blocked submissions keep distinct result IDs, including after cancellation."""
    started = asyncio.Event()
    release = asyncio.Event()

    async def handle(value: int) -> int:
        started.set()
        await release.wait()
        return value * 2

    engine = Aqute(
        handle,
        1,
        input_task_queue_size=1,
        result_queue=asyncio.Queue(0),
        use_priority_queue=use_priority_queue,
    )

    async def submit(value: int, submitting: asyncio.Event) -> str:
        submitting.set()
        return await engine.add_task(value)

    # The deadline detects a stranded result or completion, not elapsed performance.
    async with asyncio.timeout(5), engine, asyncio.TaskGroup() as group:
        try:
            ids = {0: await engine.add_task(0)}
            await started.wait()
            ids[1] = await engine.add_task(1)
            assert engine.counters.pending == 1

            blocked = {}
            for value in (2, 3):
                submitting = asyncio.Event()
                blocked[value] = group.create_task(submit(value, submitting))
                await submitting.wait()
                assert not blocked[value].done()

            if cancel_blocked:
                cancelled = blocked.pop(2)
                cancelled.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await cancelled
                submitting = asyncio.Event()
                blocked[4] = group.create_task(submit(4, submitting))
                await submitting.wait()
                assert not blocked[4].done()

            release.set()
            for value, submission in blocked.items():
                ids[value] = await submission
            ids[5] = await engine.add_task(5)
            ids[6] = await engine.add_task(6, task_id="caller-id")
            assert ids[6] == "caller-id"
            first_result = await engine.get_result()
            await engine.finish()
            results = [first_result, *engine.drain_results()]

            assert len(set(ids.values())) == len(ids)
            assert sorted(
                (task.data, task.task_id, task.unwrap()) for task in results
            ) == [(value, task_id, value * 2) for value, task_id in sorted(ids.items())]
        finally:
            release.set()

    # A completed manual run must leave ordered helpers usable on the same engine.
    results = await engine.process_all([8, 6, 7])
    assert [task.unwrap() for task in results] == [16, 12, 14]
