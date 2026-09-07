import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator, Callable, Coroutine, Iterable
from types import TracebackType
from typing import (
    Any,
    Generic,
    Self,
)

from aqute.errors import AquteError, AquteTooManyTasksFailedError
from aqute.ratelimiter import RateLimiter
from aqute.task import AquteTask, AquteTaskQueueType, TData, TResult
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
        )

        self._added_tasks_count = 0
        self._finished_tasks_count = 0

        self._all_tasks_added = False
        self._load_changed = asyncio.Event()

        self._specific_errors_to_retry = specific_errors_to_retry
        self._errors_to_not_retry = errors_to_not_retry

        self._start_timeout_seconds = start_timeout_seconds

        self.aiotask_of_run_load: asyncio.Task[None] | None = None

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
            self.aiotask_of_run_load = asyncio.create_task(
                self._run_load(), name="aqute-load"
            )

        return self.aiotask_of_run_load

    async def wait_till_end(self) -> None:
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
        self.set_all_tasks_added()
        await self.aiotask_of_run_load
        logger.debug("Aqute load task ended")

    async def start_and_wait(self) -> None:
        """
        Initiates Aqute's processing and awaits its completion.

        Triggers the start of Aqute's main processing and then waits for all
        tasks to finish. Ensures that all processing completes before exiting.
        """
        self.start()
        await self.wait_till_end()

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

    def set_all_tasks_added(self) -> None:
        """
        Sets the internal flag to indicate all tasks have been added.
        """
        if self._all_tasks_added:
            return
        logger.debug("Set all tasks added flag to: True")
        self._all_tasks_added = True
        self._load_changed.set()

    async def apply_to_each(
        self, tasks_data: Iterable[TData]
    ) -> AsyncIterator[AquteTask[TData, TResult]]:
        """
        Asynchronously processes each task from the provided iterable.

        Each piece of data in `tasks_data` is converted into a task and added
        for processing. Once all tasks are added, results are yielded as they
        complete.

        Args:
            tasks_data: Iterable containing data for each task.

        Returns:
            An async iterator yielding results as `AquteTask` objects.
        """
        for data in tasks_data:
            await self.add_task(data)
        self.set_all_tasks_added()
        exposed = 0
        async with self:
            while exposed != self._added_tasks_count:
                yield await self.get_task_result()
                exposed += 1

    async def apply_to_all(
        self, tasks_data: Iterable[TData]
    ) -> list[AquteTask[TData, TResult]]:
        """
        Asynchronously processes all tasks from the provided iterable.

        Each item in `tasks_data` is added as a task for processing. The method
        waits until all tasks are completed. Results are collected and returned
        in a list, maintaining the order of the input iterable.

        Args:
            tasks_data: Iterable containing data items for the tasks.

        Returns:
            A list of `AquteTask` objects with results, ordered as in the
            input iterable.
        """
        for data in tasks_data:
            await self.add_task(data)

        self.set_all_tasks_added()

        result: list[AquteTask[TData, TResult]] = []

        exposed = 0

        async with self:
            while exposed != self._added_tasks_count:
                task_result = await self.get_task_result()
                result.append(task_result)
                exposed += 1

        return sorted(result, key=lambda task: int(task.task_id))

    async def get_task_result(self) -> AquteTask[TData, TResult]:
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
        result = asyncio.create_task(self.result_queue.get())
        try:
            await asyncio.wait((result, load), return_when=asyncio.FIRST_COMPLETED)
            if result.done():
                return result.result()
            if load.cancelled():
                raise AquteError("Load was cancelled; call stop first")
            await load
            raise AquteError("Load finished without another task result")
        finally:
            result.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await result

    def extract_all_results(self) -> list[AquteTask[TData, TResult]]:
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
                with contextlib.suppress(asyncio.CancelledError):
                    await self.aiotask_of_run_load
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
                while self._added_tasks_count == self._finished_tasks_count:
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
            await self._process_error_task(handled_task)
            return

        handled_task.success = True
        await self._put_task_to_result(handled_task)

        logger.debug(
            f"Finished task {task_id} "
            f"{self._added_tasks_count, self._finished_tasks_count}"
        )

    async def _process_error_task(self, task: AquteTask[TData, TResult]) -> None:
        task._remaining_tries -= 1
        task_id = task.task_id

        if not self._should_retry_task(task):
            await self._put_task_to_result(task)
            logger.debug(
                f"Task {task_id} is not retriable, finishing task "
                f"{self._added_tasks_count, self._finished_tasks_count}"
            )
            self._check_failed_tasks_limit()
            return

        task.error = None
        logger.debug(
            f"Retrying task {task_id} with remaining tries {task._remaining_tries}"
        )
        await self._foreman.add_task(task)

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
