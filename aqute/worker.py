import asyncio
import contextlib
import logging
import math
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any, Generic

from aqute.errors import AquteError, AquteTaskTimeoutError
from aqute.ratelimiter import RateLimiter
from aqute.task import AquteTask, AquteTaskQueueType, TData, TResult

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
        _results_changed: asyncio.Event | None = None,
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
        self._results_changed = _results_changed
        self._shutdown_requested: asyncio.Event | None = None

    async def run(self) -> None:
        while True:
            task = await self.input_q.get()
            self._counters.running += 1
            try:
                logger.debug("Worker %s got task %s", self.name, task.task_id)
                await self.handle_task(task)
            except asyncio.CancelledError as exc:
                worker = asyncio.current_task()
                assert worker is not None
                if worker.cancelling():
                    raise
                # TaskGroup ignores child cancellation. Fail the run when an
                # awaited operation cancels without a request to stop this worker.
                raise AquteError(
                    f"Worker {self.name} on task {task.task_id} cancelled unexpectedly"
                ) from exc
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
        if self._results_changed is not None:
            self._results_changed.set()

    async def _handle_attempt(
        self, task: AquteTask[TData, TResult], *, is_retry: bool
    ) -> None:
        if self.rate_limiter:
            await self.rate_limiter.acquire(name=self.name, task=task)
        timeout: asyncio.Timeout | None = None
        try:
            if self.task_timeout_seconds is not None and self.task_timeout_seconds <= 0:
                raise AquteTaskTimeoutError(f"Task {task.task_id} timed out")
            if self.task_timeout_seconds is None:
                if is_retry:
                    self._counters.retries += 1
                task.result = await self.handle_coro(task.data)
            else:
                async with asyncio.timeout(self.task_timeout_seconds) as timeout:
                    if is_retry:
                        self._counters.retries += 1
                    task.result = await self.handle_coro(task.data)
        except Exception as exc:
            if (
                self._shutdown_requested is not None
                and self._shutdown_requested.is_set()
            ):
                raise
            if (
                isinstance(exc, TimeoutError)
                and timeout is not None
                and timeout.expired()
            ):
                exc = AquteTaskTimeoutError(f"Task {task.task_id} timed out")
            logger.warning(
                f"Worker {self.name} on {task.task_id} got error: "
                f"{exc.__class__}: {exc}"
            )
            task.error = exc


class Foreman(Generic[TData, TResult]):
    """Manage asyncio workers with shared input and output queues.

    Submit `AquteTask` objects and inspect `error` and `result` after processing.
    This layer does not set `success`; successful results cannot be read with
    `unwrap()`. Prefer `Aqute` for the managed terminal-result contract.
    """

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
        """Configure workers without starting their asyncio tasks.

        Args:
            handle_coro: Async handler called with `task.data` on each attempt.
            workers_count: Maximum occupied workers. Must be at least one.
            rate_limiter: Limiter acquired before every attempt, including retries.
                `None` disables rate limiting.
            input_task_queue_size: Maximum pending input items.
                Nonpositive values mean unlimited.
            use_priority_queue: Order pending inputs by task priority.
                Use `Aqute.add_task()` to configure priority through a public API.
            task_timeout_seconds: Timeout in seconds per handler attempt,
                excluding limiter and retry waits. `None` disables it.
                Expiry is stored as `AquteTaskTimeoutError`; a nonpositive value
                prevents handler invocation.
            output_task_queue_size: Maximum buffered worker results.
                Nonpositive values mean unlimited. Consume results while
                awaiting `finalize()`.
            retry_filter: Decide whether a failed task gets another attempt after
                decrementing its internal remaining-tries value. `None` disables
                retries. This callback owns the retry policy; `Foreman` does not
                impose a retry limit. Use `Aqute` for a configured retry budget.
            retry_delay: Map the 1-based failed-attempt number and exception to
                finite, nonnegative seconds before a permitted retry.
                `None` means zero delay. Invalid values or callback errors fail
                the worker group.

        Raises:
            ValueError: If `workers_count` is less than one.
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
        """Start worker asyncio tasks on the running event loop.

        Repeated calls do not start another group before cleanup resets it.

        Raises:
            RuntimeError: If no event loop is running.
        """
        if self._worker_run is None:
            self._worker_run = asyncio.create_task(
                self._run_workers(), name="aqute-workers"
            )
            changed = self._results_changed
            self._worker_run.add_done_callback(lambda _: changed.set())

    async def add_task(self, task: AquteTask[TData, TResult]) -> None:
        """Submit a task, waiting for input queue capacity.

        Start workers before filling a finite input queue.
        Cancellation can race completed admission. A cancelled submission does
        not prove that the task was not accepted or prevent its processing.

        Args:
            task: The task to be processed.

        Raises:
            RuntimeError: If workers finished normally before admission.
            ExceptionGroup: If the worker group failed.
            asyncio.CancelledError: If the caller or worker group was cancelled.
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
            # Some Python 3.14 releases retain this task on a pending supervisor.
            # https://github.com/python/cpython/issues/152569
            discard = getattr(asyncio, "future_discard_from_awaited_by", None)
            if discard is not None:
                discard(run, asyncio.current_task())
            admission.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await admission

    async def get_handled_task(self) -> AquteTask[TData, TResult]:
        """Return the next processed task, waiting for worker output.

        Without a started worker group, this waits on the output queue.
        Results have `result` or `error` set, but `success` is not set.
        Consume results before cleanup resets the queues.

        Returns:
            The next processed task in output queue order.

        Raises:
            RuntimeError: If the worker group finished normally without a result.
            ExceptionGroup: If no result is queued and the worker group failed.
            asyncio.CancelledError: If the caller is cancelled, or no result is
                queued and the worker group was cancelled.
        """
        # Keep this run's queue and notification if finalize/stop resets the pool.
        queue, changed, run = self.out_queue, self._results_changed, self._worker_run
        if run is None:
            return await queue.get()
        while queue.empty():
            if run.done():
                await run
                raise RuntimeError("Workers finished without another result")
            changed.clear()
            await changed.wait()
        return queue.get_nowait()

    async def finalize(self) -> None:
        """Finish admitted work, stop workers, and reset the queues.

        Stop producers first. Consume finite output queues concurrently to
        avoid blocking completion. Reset discards unconsumed output, including
        when worker failure propagates. Does nothing if workers never started.

        Raises:
            ExceptionGroup: If the worker group failed.
            asyncio.CancelledError: If the caller or worker group was cancelled.
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
        Pending inputs and unconsumed output are discarded. Unlike `finalize()`,
        this does not wait for admitted work to complete.

        Raises:
            ExceptionGroup: If the worker group or cancellation cleanup failed,
                including when the caller requested cancellation.
            asyncio.CancelledError: If the caller is cancelled during cleanup.
        """
        try:
            if self._worker_run is not None:
                self._worker_run.cancel()
                caller = asyncio.current_task()
                assert caller is not None
                cancelling = caller.cancelling()
                try:
                    await self._worker_run
                except asyncio.CancelledError:
                    if caller.cancelling() > cancelling:
                        raise
        finally:
            self.reset()

    def reset(
        self,
        *,
        input_task_queue_size: int | None = None,
        output_task_queue_size: int | None = None,
    ) -> None:
        """
        Resets the Foreman state.

        Via re-initializing worker input and output queues,
        re-creating the worker instances, and clearing
        the worker supervisor. Call only after workers have stopped.
        Capacity overrides apply to this reset only; later resets use the
        constructor capacities.
        """
        logger.debug("Resetting workers")
        self.in_queue = self._create_task_queue(
            size=(
                self._input_task_queue_size
                if input_task_queue_size is None
                else input_task_queue_size
            )
        )
        self.out_queue = asyncio.Queue(
            maxsize=(
                self._output_task_queue_size
                if output_task_queue_size is None
                else output_task_queue_size
            )
        )
        self._results_changed = asyncio.Event()
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
                _results_changed=self._results_changed,
            )
            for i in range(self._workers_count)
        ]
        self._worker_run = None
        self._closing = asyncio.Event()

    async def _run_workers(self) -> None:
        shutdown_requested = asyncio.Event()
        for worker in self._workers:
            worker._shutdown_requested = shutdown_requested

        async def run_worker(worker: Worker[TData, TResult]) -> None:
            try:
                await worker.run()
            except BaseException:
                # Publish before TaskGroup cancels sibling workers.
                shutdown_requested.set()
                raise

        async with asyncio.TaskGroup() as group:
            jobs = [
                group.create_task(run_worker(w), name=w.name) for w in self._workers
            ]
            try:
                await self._closing.wait()
                await self.in_queue.join()
            finally:
                # Mark owner cancellation before workers begin handler cleanup.
                shutdown_requested.set()
            for job in jobs:
                job.cancel()

    def _create_task_queue(self, size: int = 0) -> AquteTaskQueueType[TData, TResult]:
        if self._use_priority_queue:
            return asyncio.PriorityQueue(maxsize=size)
        return asyncio.Queue(maxsize=size)
