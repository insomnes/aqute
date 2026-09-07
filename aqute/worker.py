import asyncio
import contextlib
import logging
import math
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any, Generic

from aqute.errors import AquteTaskTimeoutError
from aqute.ratelimiter import RateLimiter
from aqute.task import END_MARKER, AquteTask, AquteTaskQueueType, TData, TResult

logger = logging.getLogger("aqute.worker")


@dataclass
class _WorkerCounters:
    running: int = 0
    succeeded: int = 0
    failed: int = 0
    retries: int = 0

    def reset(self) -> None:
        self.running = self.succeeded = self.failed = self.retries = 0


class Worker(Generic[TData, TResult]):
    def __init__(
        self,
        name: str,
        handle_coro: Callable[[TData], Coroutine[Any, Any, TResult]],
        input_q: AquteTaskQueueType[TData, TResult],
        output_q: AquteTaskQueueType[TData, TResult],
        rate_limiter: RateLimiter | None = None,
        task_timeout_seconds: int | float | None = None,
        *,
        retry_filter: Callable[[AquteTask[TData, TResult]], bool] | None = None,
        retry_delay: Callable[[int, Exception], float] | None = None,
        _counters: _WorkerCounters | None = None,
    ):
        self.handle_coro = handle_coro
        self.input_q = input_q
        self.output_q = output_q
        self.name = name

        self.rate_limiter = rate_limiter
        self.task_timeout_seconds = task_timeout_seconds
        self._retry_filter = retry_filter
        self._retry_delay = retry_delay
        self._counters = _counters if _counters is not None else _WorkerCounters()

    async def run(self) -> None:
        while True:
            task = await self.input_q.get()
            self._counters.running += 1
            try:
                logger.debug(f"Worker {self.name} got task {task.task_id}")
                if task.data is END_MARKER:
                    return
                await self.handle_task(task)
            finally:
                self._counters.running -= 1
                self.input_q.task_done()

    async def handle_task(self, task: AquteTask[TData, TResult]) -> None:
        failed_attempt = 0
        while True:
            await self._handle_attempt(task, is_retry=failed_attempt > 0)
            error = task.error
            if error is None or self._retry_filter is None:
                break
            failed_attempt += 1
            task._remaining_tries -= 1
            if not self._retry_filter(task):
                break
            delay = 0.0
            if self._retry_delay is not None:
                delay = self._retry_delay(failed_attempt, error)
                if not math.isfinite(delay) or delay < 0:
                    raise ValueError(
                        "retry_delay must return finite, nonnegative seconds"
                    )
            task.error = None
            # Retry in this worker: re-enqueuing from the collector can deadlock
            # when both input and output queues are full.
            await asyncio.sleep(delay)
        if task.error is None:
            self._counters.succeeded += 1
        else:
            self._counters.failed += 1
        await self.output_q.put(task)

    async def _handle_attempt(
        self, task: AquteTask[TData, TResult], *, is_retry: bool
    ) -> None:
        if self.rate_limiter:
            await self.rate_limiter.acquire(name=self.name, task=task)
        try:
            if self.task_timeout_seconds is not None and self.task_timeout_seconds <= 0:
                raise TimeoutError
            async with asyncio.timeout(self.task_timeout_seconds):
                if is_retry:
                    self._counters.retries += 1
                task.result = await self.handle_coro(task.data)
        except TimeoutError:
            logger.warning(
                f"Worker {self.name} on {task.task_id} timed out after "
                f"{self.task_timeout_seconds} seconds"
            )
            task.error = AquteTaskTimeoutError(f"Task {task.task_id} timed out")
        except Exception as exc:
            logger.warning(
                f"Worker {self.name} on {task.task_id} got error: "
                f"{exc.__class__}: {exc}"
            )
            task.error = exc


class Foreman(Generic[TData, TResult]):
    def __init__(
        self,
        handle_coro: Callable[[TData], Coroutine[Any, Any, TResult]],
        workers_count: int,
        rate_limiter: RateLimiter | None = None,
        input_task_queue_size: int = 0,
        use_priority_queue: bool = False,
        task_timeout_seconds: int | float | None = None,
        *,
        output_task_queue_size: int = 0,
        retry_filter: Callable[[AquteTask[TData, TResult]], bool] | None = None,
        retry_delay: Callable[[int, Exception], float] | None = None,
    ):
        """
        Initialize a worker pool with a shared input queue.

        Args:
            handle_coro: Coroutine designated for task processing.
            workers_count: Number of workers. Must be at least one.
            rate_limiter (optional): Tool to control processing rate. If not given,
                processing won't be rate-limited.
            input_task_queue_size (optional): Maximum size of the input queue. Defaults
                to 0, which means no limit.
            use_priority_queue (optional): Whether to use a priority queue for input.
                Defaults to False.
            task_timeout_seconds (optional): Timeout for task handler coroutine wait.
                Defaults to None. AquteTaskTimeoutError will be raised
                if task processing takes longer than this value.
            output_task_queue_size (optional): Maximum buffered worker results.
                Zero means unlimited. Consume results while waiting for finalize.
            retry_filter (optional): Decide whether a failed task gets another
                attempt after decrementing its remaining tries. Defaults to None.
            retry_delay (optional): Select a delay before each permitted retry.
                Receives the 1-based failed-attempt number and its exception.
        """
        if workers_count < 1:
            raise ValueError("workers_count must be at least 1")
        self._handle_coro = handle_coro
        self._workers_count = workers_count
        self._rate_limiter = rate_limiter

        self._input_task_queue_size = input_task_queue_size
        self._use_priority_queue = use_priority_queue

        self._task_timeout_seconds = task_timeout_seconds
        self._output_task_queue_size = output_task_queue_size
        self._retry_filter = retry_filter
        self._retry_delay = retry_delay
        self._counters = _WorkerCounters()

        self.in_queue: AquteTaskQueueType[TData, TResult] = self._create_task_queue(
            input_task_queue_size
        )
        self.out_queue: AquteTaskQueueType[TData, TResult] = asyncio.Queue()
        self._workers: list[Worker[TData, TResult]] = []

        self._worker_run: asyncio.Task[None] | None = None
        self._closing = asyncio.Event()
        self.reset()

    def start(self) -> None:
        """
        Initiates the worker processes.

        If the workers haven't been initialized yet, they'll be
        set to start processing tasks.
        """
        if self._worker_run is None:
            self._worker_run = asyncio.create_task(
                self._run_workers(), name="aqute-workers"
            )

    async def add_task(self, task: AquteTask[TData, TResult]) -> None:
        """
        Adds a specified task to the input queue for processing.

        Args:
            task: The task to be processed.
        """
        run = self._worker_run
        if run is not None and run.done():
            await run
            raise RuntimeError("Workers finished before the task could be queued")
        if run is None or not self.in_queue.full():
            await self.in_queue.put(task)
            return
        admission = asyncio.create_task(self.in_queue.put(task))
        try:
            await asyncio.wait((admission, run), return_when=asyncio.FIRST_COMPLETED)
            if run.done():
                await run
                raise RuntimeError("Workers finished before the task could be queued")
            await admission
        finally:
            admission.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await admission

    async def get_handled_task(self) -> AquteTask[TData, TResult]:
        """
        Retrieves a processed task from the worker's output queue.

        Returns:
            AquteTask: The processed task from the queue.
        """
        if not self.out_queue.empty():
            return self.out_queue.get_nowait()
        if self._worker_run is None:
            return await self.out_queue.get()
        result = asyncio.create_task(self.out_queue.get())
        try:
            await asyncio.wait(
                (result, self._worker_run), return_when=asyncio.FIRST_COMPLETED
            )
            if result.done():
                return result.result()
            await self._worker_run
            raise RuntimeError("Workers finished without another result")
        finally:
            result.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await result

    async def finalize(self) -> None:
        """
        Drain queued tasks and wait for all workers to exit, then reset.
        """
        if self._worker_run is None:
            return
        self._closing.set()
        try:
            await self._worker_run
        finally:
            self.reset()

    async def stop(self) -> None:
        """
        Cancel workers and wait for their cleanup, then reset the queues.

        Handlers and rate limiters must cooperate with asyncio cancellation.
        """
        try:
            if self._worker_run is not None:
                self._worker_run.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._worker_run
        finally:
            self.reset()

    def reset(self) -> None:
        """
        Resets the Foreman state.

        Via re-initializing worker input and output queues,
        re-creating the worker instances, and clearing
        the worker supervisor. Call only after workers have stopped.
        """
        logger.debug("Resetting workers")
        self.in_queue = self._create_task_queue(size=self._input_task_queue_size)
        self.out_queue = asyncio.Queue(maxsize=self._output_task_queue_size)
        self._workers = [
            Worker(
                name=f"worker_{i}",
                handle_coro=self._handle_coro,
                input_q=self.in_queue,
                output_q=self.out_queue,
                rate_limiter=self._rate_limiter,
                task_timeout_seconds=self._task_timeout_seconds,
                retry_filter=self._retry_filter,
                retry_delay=self._retry_delay,
                _counters=self._counters,
            )
            for i in range(self._workers_count)
        ]
        self._worker_run = None
        self._closing = asyncio.Event()

    async def _run_workers(self) -> None:
        async with asyncio.TaskGroup() as group:
            jobs = [group.create_task(w.run(), name=w.name) for w in self._workers]
            await self._closing.wait()
            await self.in_queue.join()
            for job in jobs:
                job.cancel()

    def _create_task_queue(self, size: int = 0) -> AquteTaskQueueType[TData, TResult]:
        if self._use_priority_queue:
            return asyncio.PriorityQueue(maxsize=size)
        return asyncio.Queue(maxsize=size)
