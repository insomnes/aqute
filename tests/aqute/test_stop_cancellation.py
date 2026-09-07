import asyncio
import contextlib

import pytest

from aqute import Aqute, AquteTask
from aqute.worker import Foreman


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["engine", "foreman"])
async def test_stop_preserves_the_callers_deadline(kind):
    """A deadline during handler cleanup must reach the caller as TimeoutError."""
    started = asyncio.Event()
    cleaning = asyncio.Event()
    release = asyncio.Event()

    async def handle(value: int) -> int:
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaning.set()
            await release.wait()
        return value

    if kind == "engine":
        pool = Aqute(handle, 1)
        pool.start()
        await pool.add_task(1)
    else:
        pool = Foreman(handle, 1)
        pool.start()
        await pool.add_task(AquteTask(1, "1"))
    await started.wait()

    async def stop_with_deadline():
        async with asyncio.timeout(0.02):
            await pool.stop()

    stopping = asyncio.create_task(stop_with_deadline())
    try:
        async with asyncio.timeout(1):
            await cleaning.wait()
            await asyncio.sleep(0.05)
            release.set()
            with pytest.raises(TimeoutError):
                await stopping
    finally:
        release.set()
        stopping.cancel()
        with contextlib.suppress(asyncio.CancelledError, TimeoutError):
            await stopping
        await pool.stop()


@pytest.mark.asyncio
async def test_iterator_close_preserves_the_callers_deadline():
    """A deadline during source cleanup must propagate from the public iterator."""
    release = asyncio.Event()
    cleaning = asyncio.Event()
    closed = asyncio.Event()

    async def source():
        try:
            yield 1
            await asyncio.Event().wait()
        finally:
            cleaning.set()
            try:
                await release.wait()
            finally:
                closed.set()

    async def handle(value: int) -> int:
        return value

    engine = Aqute(handle, 1)
    stream = engine.iter_results(source())
    assert (await anext(stream)).result == 1

    async def close_with_deadline():
        async with asyncio.timeout(0.02):
            await stream.aclose()

    closing = asyncio.create_task(close_with_deadline())
    try:
        async with asyncio.timeout(1):
            await cleaning.wait()
            await asyncio.sleep(0.05)
            release.set()
            with pytest.raises(TimeoutError):
                await closing
        assert closed.is_set()
        assert (await engine.process_all([2]))[0].result == 2
    finally:
        release.set()
        closing.cancel()
        with contextlib.suppress(asyncio.CancelledError, TimeoutError):
            await closing
        await stream.aclose()
        await engine.stop()
