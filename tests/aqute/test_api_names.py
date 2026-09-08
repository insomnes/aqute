import asyncio
import contextlib
import warnings
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import AbstractAsyncContextManager
from pathlib import Path
from typing import assert_type

import pytest

from aqute import Aqute, AquteError, AquteTask


async def stringify(value: int) -> str:
    return str(value)


@pytest.mark.asyncio
async def test_canonical_methods_preserve_types_without_deprecation_warnings():
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        async with Aqute(stringify, 1, result_queue=asyncio.Queue(0)) as engine:
            assert_type(engine, Aqute[int, str])
            await engine.add_task(1)
            await engine.add_task(2)
            engine.finish_submitting()
            await engine.finish()
            result = await engine.get_result()
            assert_type(result, AquteTask[int, str])
            assert result.result == "1"
            remaining = engine.drain_results()
            assert_type(remaining, list[AquteTask[int, str]])
            assert [item.result for item in remaining] == ["2"]
        await engine.add_task(3)
        try:
            await engine.run()
            assert (await engine.get_result()).result == "3"
        finally:
            await engine.stop()
        results = await engine.process_all([4, 5])
        assert_type(results, list[AquteTask[int, str]])
        assert [item.result for item in results] == ["4", "5"]
        context = engine.iter_results([6])
        assert_type(
            context, AbstractAsyncContextManager[AsyncIterator[AquteTask[int, str]]]
        )
        async with context as stream:
            assert_type(stream, AsyncIterator[AquteTask[int, str]])
            assert [item.result async for item in stream] == ["6"]


@pytest.mark.asyncio
async def test_legacy_lifecycle_wrappers_warn_at_the_caller_and_keep_results():
    async with Aqute(stringify, 1, result_queue=asyncio.Queue(0)) as engine:
        await engine.add_task(1)
        await engine.add_task(2)
        with pytest.warns(DeprecationWarning, match="finish_submitting") as signals:
            engine.set_all_tasks_added()
        with pytest.warns(DeprecationWarning, match="use finish") as finishing:
            await engine.wait_till_end()
        with pytest.warns(DeprecationWarning, match="use get_result") as getting:
            result = await engine.get_task_result()
            assert_type(result, AquteTask[int, str])
            assert result.result == "1"
        with pytest.warns(DeprecationWarning, match="use drain_results") as draining:
            remaining = engine.extract_all_results()
            assert_type(remaining, list[AquteTask[int, str]])
            assert [item.result for item in remaining] == ["2"]
    await engine.add_task(3)
    try:
        with pytest.warns(DeprecationWarning, match="use run") as running:
            await engine.start_and_wait()
        assert (await engine.get_result()).result == "3"
    finally:
        await engine.stop()
    for caught in [signals, finishing, getting, draining, running]:
        assert len(caught) == 1
        assert Path(caught[0].filename) == Path(__file__)


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy", [False, True])
async def test_helper_names_preserve_order_and_error_values(legacy):
    async def handler(value: int) -> str:
        if value == 2:
            raise ValueError("failed")
        return str(value)

    engine = Aqute(handler, 2)
    if legacy:
        with pytest.warns(DeprecationWarning, match="use process_all") as caught:
            results = await engine.apply_to_all([1, 2, 3])
        assert len(caught) == 1
        assert Path(caught[0].filename) == Path(__file__)
        assert_type(results, list[AquteTask[int, str]])
    else:
        results = await engine.process_all([1, 2, 3])
    assert [item.data for item in results] == [1, 2, 3]
    assert [item.result for item in results] == ["1", None, "3"]
    assert isinstance(results[1].error, ValueError)


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy", [False, True])
async def test_iterator_names_close_the_owned_work(legacy):
    waiting = asyncio.Event()
    cleaned = asyncio.Event()

    async def source():
        try:
            yield 1
            waiting.set()
            await asyncio.Event().wait()
        finally:
            cleaned.set()

    engine = Aqute(stringify, 1)
    if legacy:
        with pytest.warns(DeprecationWarning, match="use iter_results") as caught:
            legacy_stream = engine.apply_to_each(source())
        assert len(caught) == 1
        assert Path(caught[0].filename) == Path(__file__)
        assert_type(legacy_stream, AsyncGenerator[AquteTask[int, str]])
        context = contextlib.aclosing(legacy_stream)
    else:
        context = engine.iter_results(source())
    async with asyncio.timeout(1):
        async with context as stream:
            assert (await anext(stream)).result == "1"
            await waiting.wait()
    assert cleaned.is_set()
    assert (await engine.process_all([2]))[0].result == "2"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method", ["finish", "wait_till_end", "get_result", "get_task_result"]
)
async def test_lifecycle_aliases_preserve_not_started_errors(method):
    engine = Aqute(stringify, 1)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        with pytest.raises(AquteError, match="Cannot"):
            await getattr(engine, method)()
