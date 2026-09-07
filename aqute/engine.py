import asyncio
import contextlib
import logging
import warnings
from collections.abc import (
    AsyncGenerator,
    AsyncIterable,
    AsyncIterator,
    Callable,
    Coroutine,
    Generator,
    Iterable,
)
from types import TracebackType
from typing import (
    Any,
    Generic,
    Self,
)

from aqute.errors import AquteError, AquteTooManyTasksFailedError
from aqute.ratelimiter import RateLimiter
from aqute.task import AquteCounters, AquteTask, AquteTaskQueueType, TData, TResult
from aqute.worker import Foreman

logger = logging.getLogger("aqute")


class Aqute(Generic[TData, TResult]):
    def __init__(
        self,
        handle_coro: Callable[[TData], Coroutine[Any, Any, TResult]],
        workers_count: int,
        *,
        rate_limiter: RateLimiter | None = None,
        result_queue: AquteTaskQueueType[TData, TResult] | None = None,
        retry_count: int = 0,
        retry_delay: Callable[[int, Exception], float] | None = None,
        specific_errors_to_retry: tuple[type[Exception], ...]
        | type[Exception]
        | None = None,
        errors_to_not_retry: tuple[type[Exception], ...]
        | type[Exception]
        | None = None,
        start_timeout_seconds: int | float | None = None,
        input_task_queue_size: int = 0,
        use_priority_queue: bool = False,
        task_timeout_seconds: int | float | None = None,
        total_failed_tasks_limit: int | None = None,
    ):
        """
        Engine for reliable running asynchronous tasks via queue with simple retry and
        rate limiting configuration.

        Args:
            handle_coro: Coroutine function used to process tasks.
            workers_count: Number of workers for processing.
            rate_limiter (optional): Limiter for processing rate.
                Defaults to None.
            result_queue (optional): Queue for task results. Defaults
                to None.
            retry_count (optional): Number of task retry attempts upon
                failure. Defaults to 0.
            retry_delay (optional): Map the 1-based failed-attempt number and
                exception to finite, nonnegative seconds. Defaults to zero delay.
                The delay occupies a worker but is outside the handler timeout.
            specific_errors_to_retry (optional): Exceptions triggering
                task retry. Defaults to None, so every error is retried.
            errors_to_not_retry (optional): Exceptions that should not be
                retried. This option takes precedence over specific_errors_to_retry.
                Defaults to None.
            start_timeout_seconds (optional): Wait time before failing after start
                if no tasks were added. Defaults to None.
            input_task_queue_size (optional): Max size of the input task
                queue. 0 indicates unlimited. Defaults to 0.
            use_priority_queue (optional): Use priority queue for tasks.
                Defaults to False.
            task_timeout_seconds (optional): Timeout for task handler coroutine wait.
                Defaults to None. AquteTaskTimeoutError will be raised
                if task processing takes longer than this value. To not retry task
                on timeout, add this exception to `errors_to_not_retry`.
            total_failed_tasks_limit (optional): Maximum failed tasks count before
                stopping processing. Defaults to None.
        """
        self.result_queue: AquteTaskQueueType[TData, TResult] = (
            result_queue or asyncio.Queue()
        )

        self._task_tries_count = max(retry_count, 0) + 1
        self._input_task_queue_size = max(input_task_queue_size, 0)

        self._rate_limiter = rate_limiter

        self._handle_coro = handle_coro
        self._workers_count = workers_count
        self._use_priority_queue = use_priority_queue
        self._task_timeout_seconds = task_timeout_seconds

        self._failed_tasks = 0
        self._total_failed_limit = total_failed_tasks_limit

        self._foreman = Foreman(
            handle_coro=self._handle_coro,
            workers_count=self._workers_count,
            rate_limiter=self._rate_limiter,
            input_task_queue_size=self._input_task_queue_size,
            use_priority_queue=self._use_priority_queue,
            task_timeout_seconds=self._task_timeout_seconds,
            output_task_queue_size=self.result_queue.maxsize,
            retry_filter=self._should_retry_task,
            retry_delay=retry_delay,
        )

        self._added_tasks_count = 0
        self._finished_tasks_count = 0

        self._all_tasks_added = False
        self._load_changed = asyncio.Event()
        self._results_changed = asyncio.Event()

        self._specific_errors_to_retry = specific_errors_to_retry
        self._errors_to_not_retry = errors_to_not_retry

        self._start_timeout_seconds = start_timeout_seconds

        self.aiotask_of_run_load: asyncio.Task[None] | None = None

    @property
    def counters(self) -> AquteCounters:
        """Return an immutable snapshot for this run, reset after stop().

        pending counts admitted tasks awaiting assignment, excluding blocked
        submissions. running counts occupied workers, including rate-limit,
        retry-delay, and result-publication waits. succeeded/failed count terminal
        handler outcomes before publication. retries counts additional handler
        invocations, excluding retries still waiting to start. Reading results
        does not change counts; retained results are excluded from later runs.
        """
        counts = self._foreman._counters
        return AquteCounters(
            pending=self._foreman.in_queue.qsize(),
            running=counts.running,
            succeeded=counts.succeeded,
            failed=counts.failed,
            retries=counts.retries,
        )

    def start(self) -> asyncio.Task[None]:
        """
        Starts the Aqute processing.

        If the main load runner task (`aiotask_of_run_load`) is not already
        initiated, it creates and starts the task.

        Returns:
            asyncio.Task: The main processing task (`aiotask_of_run_load`).
        """
        logger.debug("Starting aqute")
        if self.aiotask_of_run_load is None:
            self._results_changed = asyncio.Event()
            self.aiotask_of_run_load = asyncio.create_task(
                self._run_load(), name="aqute-load"
            )
            changed = self._results_changed
            self.aiotask_of_run_load.add_done_callback(lambda _: changed.set())

        return self.aiotask_of_run_load

    async def finish(self) -> None:
        """
        Awaits the completion of Aqute's main processing task.

        If the `aiotask_of_run_load` hasn't been started, raises an error. If the
        task is active, this method waits until it completes.

        Raises:
            AquteError: If the task hasn't been initiated.
            AquteTooManyTasksFailedError: If there was limit on failed tasks
                and it was reached.

        Side Effects:
            Marks all tasks as added.
        """
        logger.debug("Waiting till aqute end")
        if self.aiotask_of_run_load is None:
            raise AquteError("Cannot wait for not started load")
        self.finish_submitting()
        await self.aiotask_of_run_load
        logger.debug("Aqute load task ended")

    async def run(self) -> None:
        """
        Initiates Aqute's processing and awaits its completion.

        Triggers the start of Aqute's main processing and then waits for all
        tasks to finish. Ensures that all processing completes before exiting.
        """
        self.start()
        await self.finish()

    async def add_task(
        self,
        task_data: TData,
        task_id: str | None = None,
        task_priority: int = 1_000_000,
    ) -> str:
        """
        Asynchronously adds a new task for processing.

        Generates a unique task_id if one isn't provided. The task is then
        forwarded to the foreman for execution and the count of added tasks is
        incremented.

        Args:
            task_data: Data for the task to process.
            task_id (optional): Identifier for the task. If not provided, it's
                auto-generated based on the added tasks count.
            task_priority (optional): Priority of the task used if priority queue is
                enabled. Lower means more prior task. Defaults to 1_000_000.

        Returns:
            The unique task_id associated with the added task.
        """
        task_id = task_id or str(self._added_tasks_count)

        task: AquteTask[TData, TResult] = AquteTask(
            data=task_data,
            task_id=task_id,
            _remaining_tries=self._task_tries_count,
            _priority=task_priority,
        )
        load = self.aiotask_of_run_load
        if load is not None and (load.done() or load.cancelling()):
            if load.cancelled() or load.cancelling():
                raise AquteError("Load was cancelled; call stop first")
            await load
            raise AquteError("Cannot add a task after load completion; call stop first")
        if load is None or not self._foreman.in_queue.full():
            await self._foreman.add_task(task)
        else:
            admission = asyncio.create_task(self._foreman.add_task(task))
            try:
                await asyncio.wait(
                    (admission, load), return_when=asyncio.FIRST_COMPLETED
                )
                if load.done():
                    if load.cancelled():
                        raise AquteError("Load was cancelled; call stop first")
                    await load
                    raise AquteError("Load finished before the task could be added")
                await admission
            finally:
                admission.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await admission

        self._added_tasks_count += 1
        self._load_changed.set()

        return task_id

    def finish_submitting(self) -> None:
        """
        Sets the internal flag to indicate all tasks have been added.
        """
        if self._all_tasks_added:
            return
        logger.debug("Set all tasks added flag to: True")
        self._all_tasks_added = True
        self._load_changed.set()

    def iter_results(
        self,
        tasks_data: Iterable[TData] | AsyncIterable[TData],
        *,
        submission_batch_size: int = 1,
    ) -> contextlib.AbstractAsyncContextManager[
        AsyncIterator[AquteTask[TData, TResult]]
    ]:
        """
        Return a context that owns a single-use, lazy result iterator.

        Produce input concurrently with processing and yield terminal results
        in completion order. Finite input and result queues bound buffering.
        Use ``async with engine.iter_results(items) as results``. First iteration
        starts processing; context entry alone does not consume or close input.
        Context exit awaits producer and worker cleanup, including after a break,
        consumer exception, or cancellation. Cleanup requires cooperative sources
        and handlers. Drain retained results before reusing the engine.
        Source exceptions propagate after cancelling owned processing.

        Args:
            tasks_data: Iterable containing data for each task.
            submission_batch_size: Positive number of inputs to admit between
                cooperative yields. Defaults to 1. Larger values can improve
                throughput at the cost of result latency. Queue backpressure
                and asynchronous input can yield sooner. Handlers still receive
                one item each; this does not bound otherwise unlimited queues.
                Batching can be slower when small input queues fill frequently.

        Returns:
            An async context manager yielding an iterator of `AquteTask` objects.

        Raises:
            ValueError: If submission_batch_size is not a positive integer.
                Validation occurs when iteration starts, before consuming input.
        """
        return contextlib.aclosing(
            self._iter_results(tasks_data, submission_batch_size=submission_batch_size)
        )

    async def _iter_results(
        self,
        tasks_data: Iterable[TData] | AsyncIterable[TData],
        *,
        submission_batch_size: int = 1,
    ) -> AsyncGenerator[AquteTask[TData, TResult]]:
        if not isinstance(submission_batch_size, int) or submission_batch_size < 1:
            raise ValueError("submission_batch_size must be a positive integer")
        async with self:
            load = self.start()
            producer = asyncio.create_task(
                self._produce_tasks(tasks_data, submission_batch_size),
                name="aqute-producer",
            )
            changed = self._results_changed
            producer.add_done_callback(lambda _: changed.set())
            exposed = 0
            try:
                while not producer.done() or exposed < self._added_tasks_count:
                    if producer.done():
                        await producer
                    if not self.result_queue.empty():
                        exposed += 1
                        yield self.result_queue.get_nowait()
                        continue
                    if load.done():
                        await load
                    changed.clear()
                    await changed.wait()
                await producer
                await load
            finally:
                producer.cancel()
                caller = asyncio.current_task()
                assert caller is not None
                cancelling = caller.cancelling()
                try:
                    await producer
                except asyncio.CancelledError:
                    if caller.cancelling() > cancelling:
                        raise

    async def _produce_tasks(
        self,
        tasks_data: Iterable[TData] | AsyncIterable[TData],
        submission_batch_size: int,
    ) -> None:
        remaining = submission_batch_size
        if isinstance(tasks_data, AsyncIterable):
            source = aiter(tasks_data)
            try:
                async for data in source:
                    await self.add_task(data)
                    remaining -= 1
                    if remaining == 0:
                        remaining = submission_batch_size
                        await asyncio.sleep(0)
            finally:
                if isinstance(source, AsyncGenerator):
                    await source.aclose()
        else:
            source = iter(tasks_data)
            try:
                for data in source:
                    await self.add_task(data)
                    remaining -= 1
                    if remaining == 0:
                        remaining = submission_batch_size
                        await asyncio.sleep(0)
            finally:
                if isinstance(source, Generator):
                    source.close()
        self.finish_submitting()

    async def process_all(
        self,
        tasks_data: Iterable[TData] | AsyncIterable[TData],
        *,
        submission_batch_size: int = 1,
    ) -> list[AquteTask[TData, TResult]]:
        """
        Asynchronously processes all tasks from the provided iterable.

        Each item in `tasks_data` is added as a task for processing. The method
        waits until all tasks are completed. Results are collected and returned
        in a list, maintaining the order of the input iterable.

        Args:
            tasks_data: Iterable containing data items for the tasks.
            submission_batch_size: Positive number of inputs to admit between
                cooperative yields; see iter_results(). Defaults to 1.

        Returns:
            A list of `AquteTask` objects with results, ordered as in the
            input iterable.

        Raises:
            ValueError: If submission_batch_size is not a positive integer.
        """
        async with self.iter_results(
            tasks_data, submission_batch_size=submission_batch_size
        ) as results:
            result = [task async for task in results]
        return sorted(result, key=lambda task: int(task.task_id))

    async def get_result(self) -> AquteTask[TData, TResult]:
        """
        Get first available task result in the result queue

        Returns:
            AquteTask with result or error set
        Raises:
            AquteError: If no load is running or a completed load has no more results.
            AquteTooManyTasksFailedError: If there was limit on failed tasks
                and it was reached.
        """
        if not self.result_queue.empty():
            return self.result_queue.get_nowait()
        load = self.aiotask_of_run_load
        if load is None:
            raise AquteError("Cannot get task result without started load")
        changed = self._results_changed
        while self.result_queue.empty():
            if load.done():
                if load.cancelled():
                    raise AquteError("Load was cancelled; call stop first")
                await load
                raise AquteError("Load finished without another task result")
            changed.clear()
            await changed.wait()
        return self.result_queue.get_nowait()

    def drain_results(self) -> list[AquteTask[TData, TResult]]:
        """
        Retrieves all the results available in the result queue.

        Extracts and returns all completed `AquteTask` objects from the result
        queue without waiting. If the queue is empty, an empty list is returned.
        Users should ensure Aqute has finished processing before calling this method.

        Returns:
            A list of `AquteTask` objects, each representing a completed task.
        """
        result = []
        while not self.result_queue.empty():
            result.append(self.result_queue.get_nowait())
        return result

    async def stop(self) -> None:
        """
        Cancel processing and wait for worker cleanup before resetting counters.

        Completed results remain available. Handlers and rate limiters must
        cooperate with asyncio cancellation. The instance can then be reused.
        """
        try:
            if self.aiotask_of_run_load is not None:
                self.aiotask_of_run_load.cancel()
                caller = asyncio.current_task()
                assert caller is not None
                cancelling = caller.cancelling()
                try:
                    await self.aiotask_of_run_load
                except asyncio.CancelledError:
                    if caller.cancelling() > cancelling:
                        raise
        finally:
            try:
                await self._foreman.stop()
            finally:
                self.aiotask_of_run_load = None
                self._added_tasks_count = 0
                self._finished_tasks_count = 0
                self._failed_tasks = 0
                self._all_tasks_added = False
                self._load_changed.clear()
                self._foreman._counters.reset()

    async def _run_load(self) -> None:
        self._foreman.start()
        try:
            await self._wait_till_can_start()
            while self._should_proceed():
                self._load_changed.clear()
                if self._added_tasks_count == self._finished_tasks_count:
                    await self._load_changed.wait()
                    continue
                handled_task = await self._foreman.get_handled_task()
                await self._process_handled_task(handled_task)

            await self._foreman.finalize()
        finally:
            await self._foreman.stop()

    def _should_proceed(self) -> bool:
        if not self._all_tasks_added:
            return True
        return self._added_tasks_count > self._finished_tasks_count

    async def _wait_till_can_start(self) -> None:
        if self._start_timeout_seconds is None:
            return

        try:
            if self._start_timeout_seconds <= 0:
                raise TimeoutError
            async with asyncio.timeout(self._start_timeout_seconds):
                while (
                    self._added_tasks_count == self._finished_tasks_count
                    and not self._all_tasks_added
                ):
                    self._load_changed.clear()
                    await self._load_changed.wait()
        except TimeoutError as exc:
            raise AquteError(
                f"Waited too long ({self._start_timeout_seconds}s) for available load"
            ) from exc

    async def _process_handled_task(
        self, handled_task: AquteTask[TData, TResult]
    ) -> None:
        task_id = handled_task.task_id

        if handled_task.error:
            await self._put_task_to_result(handled_task)
            self._check_failed_tasks_limit()
            return

        handled_task.success = True
        await self._put_task_to_result(handled_task)

        logger.debug(
            "Finished task %s (%s, %s)",
            task_id,
            self._added_tasks_count,
            self._finished_tasks_count,
        )

    def _check_failed_tasks_limit(self) -> None:
        if self._total_failed_limit is None:
            return

        self._failed_tasks += 1
        if self._failed_tasks < self._total_failed_limit:
            return

        logger.debug(f"Total failed tasks limit reached: {self._total_failed_limit}")
        raise AquteTooManyTasksFailedError(
            f"Total failed tasks limit reached: {self._total_failed_limit}"
        )

    def _should_retry_task(self, task: AquteTask[TData, TResult]) -> bool:
        if self._errors_to_not_retry and isinstance(
            task.error, self._errors_to_not_retry
        ):
            return False

        if self._specific_errors_to_retry and not isinstance(
            task.error, self._specific_errors_to_retry
        ):
            return False

        return task._remaining_tries > 0

    async def _put_task_to_result(self, task: AquteTask[TData, TResult]) -> None:
        self._finished_tasks_count += 1
        await self.result_queue.put(task)
        self._results_changed.set()

    async def __aenter__(self) -> Self:
        self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.stop()

    def set_all_tasks_added(self) -> None:
        """Deprecated; use finish_submitting()."""
        warnings.warn(
            "set_all_tasks_added() is deprecated; use finish_submitting()",
            DeprecationWarning,
            stacklevel=2,
        )
        self.finish_submitting()

    async def wait_till_end(self) -> None:
        """Deprecated; use finish()."""
        warnings.warn(
            "wait_till_end() is deprecated; use finish()",
            DeprecationWarning,
            stacklevel=2,
        )
        await self.finish()

    async def start_and_wait(self) -> None:
        """Deprecated; use run()."""
        warnings.warn(
            "start_and_wait() is deprecated; use run()",
            DeprecationWarning,
            stacklevel=2,
        )
        await self.run()

    async def get_task_result(self) -> AquteTask[TData, TResult]:
        """Deprecated; use get_result()."""
        warnings.warn(
            "get_task_result() is deprecated; use get_result()",
            DeprecationWarning,
            stacklevel=2,
        )
        return await self.get_result()

    def extract_all_results(self) -> list[AquteTask[TData, TResult]]:
        """Deprecated; use drain_results()."""
        warnings.warn(
            "extract_all_results() is deprecated; use drain_results()",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.drain_results()

    async def apply_to_all(
        self, tasks_data: Iterable[TData] | AsyncIterable[TData]
    ) -> list[AquteTask[TData, TResult]]:
        """Deprecated; use process_all()."""
        warnings.warn(
            "apply_to_all() is deprecated; use process_all()",
            DeprecationWarning,
            stacklevel=2,
        )
        return await self.process_all(tasks_data)

    def apply_to_each(
        self, tasks_data: Iterable[TData] | AsyncIterable[TData]
    ) -> AsyncGenerator[AquteTask[TData, TResult]]:
        """Deprecated generator API; close partial iteration with aclose/aclosing.

        Prefer ``async with engine.iter_results(items) as results`` in new code.
        """
        warnings.warn(
            "apply_to_each() is deprecated; use iter_results()",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._iter_results(tasks_data)
