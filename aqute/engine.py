import asyncio
import contextlib
import logging
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
    """Process asynchronous tasks with bounded queues, retries, and rate limits.

    `TData` and `TResult` are the handler's input and output types.
    Use `process_all()` for an ordered batch, `iter_results()` for a managed
    stream, or the manual lifecycle for continuous submission. Handlers,
    sources, and rate limiters must cooperate with asyncio cancellation.
    """

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
        input_task_queue_size: int | None = None,
        use_priority_queue: bool = False,
        task_timeout_seconds: int | float | None = None,
        total_failed_tasks_limit: int | None = None,
    ):
        """Configure an engine without starting processing.

        Args:
            handle_coro: Async handler called with one input item per attempt.
            workers_count: Maximum occupied workers, including limiter and retry
                waits. Must be at least one.
            rate_limiter: Attempt admission limiter, acquired again for each retry.
                `None` disables rate limiting.
            result_queue: Caller-owned queue for terminal task results. Its
                capacity also limits the internal result relay; 0 is unlimited.
                `None` gives each result queue capacity `workers_count` for
                helper and manual runs alike.
            retry_count: Extra attempts after the first handler call.
                Negative values are treated as zero.
            retry_delay: Map the 1-based failed-attempt number and exception to
                finite, nonnegative seconds. `None` means zero delay.
                The delay occupies a worker but is outside the handler timeout.
            specific_errors_to_retry: Eligible handler exception types while
                retry budget remains. `None` selects every caught handler error;
                an empty tuple selects none.
            errors_to_not_retry: Excluded exception types; takes precedence over
                `specific_errors_to_retry`. `None` excludes no types.
            start_timeout_seconds: Seconds to wait for the first input after
                start. `None` disables the limit. Prequeued tasks do not wait,
                even when the value is zero or negative.
            input_task_queue_size: Maximum pending input items. `None` uses
                `workers_count` for all runs. Positive values set a capacity;
                zero or negative values mean unlimited. Start processing
                before filling a bounded queue and consume results concurrently.
            use_priority_queue: Process admitted pending tasks by increasing
                `task_priority`. Does not preempt occupied workers.
            task_timeout_seconds: Timeout in seconds per handler attempt,
                excluding limiter and retry waits. `None` disables it.
                Expiry produces `AquteTaskTimeoutError`, subject to retry rules.
                A nonpositive value prevents handler invocation.
            total_failed_tasks_limit: Inclusive limit on collected terminal
                failures. Reaching it fails the run with
                `AquteTooManyTasksFailedError`. `None` disables the limit.
                A nonpositive value stops on the first collected failure.

        Raises:
            ValueError: If `workers_count` is less than one.
        """
        self.result_queue: AquteTaskQueueType[TData, TResult] = (
            asyncio.Queue(workers_count) if result_queue is None else result_queue
        )

        self._task_tries_count = max(retry_count, 0) + 1

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
            input_task_queue_size=(
                workers_count
                if input_task_queue_size is None
                else max(input_task_queue_size, 0)
            ),
            use_priority_queue=self._use_priority_queue,
            task_timeout_seconds=self._task_timeout_seconds,
            output_task_queue_size=self.result_queue.maxsize,
            retry_filter=self._should_retry_task,
            retry_delay=retry_delay,
        )

        self._next_task_id = 0
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

        Returns:
            Current counts, with terminal outcomes possibly overlapping `running`
                while workers wait to publish results.
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
        """Start processing and return the run task.

        Repeated calls return the same task until `stop()` resets the engine,
        even if that task has completed. Call this from a running event loop.

        Returns:
            The asyncio task that supervises this run.

        Raises:
            RuntimeError: If no event loop is running.
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
        """Signal submission completion and wait for the current run.

        Stop producers before calling this method. Keep consuming finite result
        queues while waiting. Call `stop()` afterward to reset the engine.
        Caller cancellation propagates.

        Raises:
            AquteError: If processing has not started, the run was independently
                cancelled, or the first-input timeout expired.
            AquteTooManyTasksFailedError: If the terminal failure limit was reached.
            ExceptionGroup: If a limiter, retry callback, or worker failed outside
                normal handler error capture.
        """
        logger.debug("Waiting till aqute end")
        if self.aiotask_of_run_load is None:
            raise AquteError("Cannot wait for not started load")
        self.finish_submitting()
        caller = asyncio.current_task()
        assert caller is not None
        cancelling = caller.cancelling()
        try:
            await self.aiotask_of_run_load
        except asyncio.CancelledError as exc:
            if caller.cancelling() > cancelling:
                raise
            raise AquteError("Load was cancelled; call stop first") from exc
        logger.debug("Aqute load task ended")

    async def run(self) -> None:
        """Start processing and finish work submitted before this call.

        Calls `start()` and `finish()`. For pre-submit-then-run without a
        concurrent result consumer, explicitly make both queues unlimited.
        Await `stop()` afterward to reset the engine.

        Raises:
            AquteError: If the run was independently cancelled or its first-input
                timeout expired.
            AquteTooManyTasksFailedError: If the terminal failure limit was reached.
            ExceptionGroup: If a limiter, retry callback, or worker failed.
        """
        self.start()
        await self.finish()

    async def add_task(
        self,
        task_data: TData,
        task_id: str | None = None,
        task_priority: int = 1_000_000,
    ) -> str:
        """Submit one input, waiting for queue capacity when processing is active.

        Within one run, omitted or empty IDs receive distinct generated values.
        Caller-supplied IDs are not checked for collisions and do not deduplicate
        work. Retrying a cancelled submission can therefore duplicate processing.
        Cancellation can race completed admission: a cancelled submission does
        not prove that the task was not accepted. Admission accounting includes
        accepted work, even when the submitting caller is cancelled.

        Args:
            task_data: Data for the task to process.
            task_id: Identifier for the task. If omitted or empty, it is
                generated from a per-run sequence before waiting for input capacity.
                Cancelled submissions can leave gaps in this sequence.
                `stop()` resets the sequence, so later runs can reuse IDs.
            task_priority: Priority when `use_priority_queue` is enabled.
                Lower values run first among admitted pending tasks.

        Returns:
            The ID assigned to the admitted task.

        Raises:
            AquteError: If the input queue is full before start(). Start processing
                or set input_task_queue_size=0 for unlimited pre-submission.
                Also raised when the load has completed or was cancelled.
            AquteTooManyTasksFailedError: If the active run reached its failure limit.
            ExceptionGroup: If the active run failed in a worker or retry callback.
        """
        task_id = task_id or str(self._next_task_id)

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
        if load is None and self._foreman.in_queue.full():
            raise AquteError(
                "Input queue is full before start(); call start() first "
                "or set input_task_queue_size=0 for unlimited pre-submission"
            )
        # Reserve before admission can suspend; admitted-task accounting stays separate.
        self._next_task_id += 1
        if load is None or not self._foreman.in_queue.full():
            await self._foreman.add_task(task)
            self._added_tasks_count += 1
            self._load_changed.set()
        else:

            async def admit() -> None:
                # Count the item before cancellation can resume its caller.
                # The load task below already supervises worker failure.
                await self._foreman.in_queue.put(task)
                self._added_tasks_count += 1
                self._load_changed.set()

            admission = asyncio.create_task(admit())
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

        return task_id

    def finish_submitting(self) -> None:
        """Signal that producers have finished submitting input.

        Repeated calls have no additional effect. Stop producers first: this
        signal does not reject later submissions while the run still has work.
        Use `finish()` to send the signal and wait for completion.
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

        Do not call get_result() or drain_results() during iteration: the iterator
        owns result consumption.

        Produce input concurrently with processing and yield terminal results
        in completion order. Finite input and result queues bound buffering.
        Use ``async with engine.iter_results(items) as results``. First iteration
        starts processing; context entry alone does not consume or close input.
        Context exit awaits producer and worker cleanup, including after a break,
        consumer exception, or cancellation. Cleanup requires cooperative sources
        and handlers. Drain retained results before reusing the engine.
        Source exceptions propagate after cancelling owned processing.
        Independently cancelled processing raises AquteError. Cancellation of
        the consuming caller still propagates, including during cleanup.

        Omitted queue limits use workers_count items for pending input, results,
        and the internal result relay. With finite input capacity I, result
        capacity R, and W workers, at most I + 2R + W + 3 generated items can be
        outstanding, including the currently yielded item. Defaults give 4W + 3.
        These are item bounds, not byte bounds; source and caller retention are
        excluded. Explicit input_task_queue_size=0 or result_queue=Queue(0)
        disables the corresponding limit. Supplied result queues retain their
        identity and capacity. Manual runs use the same queue limits.

        Args:
            tasks_data: Synchronous or asynchronous iterable of handler inputs.
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
            AquteError: If a run is already started, manual tasks are pending,
                or the result queue has retained results. Drain retained results
                or use a fresh engine before starting another helper run.
                Also raised for independent run cancellation or first-input timeout.
            AquteTooManyTasksFailedError: If the terminal failure limit was reached.
            ExceptionGroup: If a limiter, retry callback, or worker failed.
        """
        return contextlib.aclosing(
            self._iter_results(tasks_data, submission_batch_size=submission_batch_size)
        )

    async def _iter_results(  # noqa: C901 - keep load cancellation checks local
        self,
        tasks_data: Iterable[TData] | AsyncIterable[TData],
        *,
        submission_batch_size: int = 1,
    ) -> AsyncGenerator[AquteTask[TData, TResult]]:
        if not isinstance(submission_batch_size, int) or submission_batch_size < 1:
            raise ValueError("submission_batch_size must be a positive integer")
        with self._helper_queues():
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
                            if load.cancelled():
                                raise AquteError("Load was cancelled; call stop first")
                            await load
                        changed.clear()
                        await changed.wait()
                    await producer
                    caller = asyncio.current_task()
                    assert caller is not None
                    cancelling = caller.cancelling()
                    try:
                        await load
                    except asyncio.CancelledError as exc:
                        if caller.cancelling() > cancelling:
                            raise
                        raise AquteError("Load was cancelled; call stop first") from exc
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

    @contextlib.contextmanager
    def _helper_queues(self) -> Generator[None]:
        if self.aiotask_of_run_load is not None or self._added_tasks_count:
            raise AquteError(
                "Use a fresh engine, or complete and stop the existing run "
                "before starting a helper"
            )
        if not self.result_queue.empty():
            raise AquteError(
                "Drain retained results or use a fresh engine "
                "before starting a helper run"
            )
        yield

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
        """Process a finite input and return terminal tasks in input order.

        Each item in `tasks_data` is added as a task for processing. The method
        waits until all tasks are completed. Results are collected and returned
        in a list, maintaining the order of the input iterable.
        Internal queues follow iter_results() limits. The returned list retains
        all results, so total memory can still grow with input length.
        Handler errors remain task values. Source errors and caller cancellation
        propagate after cooperative cleanup. The engine stops before returning.

        Args:
            tasks_data: Finite synchronous or asynchronous iterable of handler inputs.
            submission_batch_size: Positive number of inputs to admit between
                cooperative yields; see iter_results(). Defaults to 1.

        Returns:
            Terminal `AquteTask` objects in input order.

        Raises:
            ValueError: If submission_batch_size is not a positive integer.
            AquteError: If a run is already started, manual tasks are pending,
                or the result queue has retained results. Drain retained results
                or use a fresh engine before starting another helper run.
                Also raised for independent run cancellation or first-input timeout.
            AquteTooManyTasksFailedError: If the terminal failure limit was reached.
            ExceptionGroup: If a limiter, retry callback, or worker failed.
        """
        async with self.iter_results(
            tasks_data, submission_batch_size=submission_batch_size
        ) as results:
            result = [task async for task in results]
        return sorted(result, key=lambda task: int(task.task_id))

    async def get_result(self) -> AquteTask[TData, TResult]:
        """Return the next queued result, waiting while the run is active.

        Results use one shared queue and are not paired with the caller's most
        recent submission. Already queued results remain readable after `stop()`.
        Do not consume results separately while a helper owns the queue.

        Returns:
            A terminal task with `success` and `result`, or `error`, set.

        Raises:
            AquteError: If no results remain and the run is absent, completed,
                independently cancelled, or failed with a first-input timeout.
            AquteTooManyTasksFailedError: If no queued results remain and the run
                reached its terminal failure limit.
            ExceptionGroup: If no queued results remain and the workers failed.
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
        """Remove all currently queued results without waiting.

        Results can arrive later while processing continues. To retrieve every
        outcome, consume concurrently until completion. Do not call this while
        a helper owns result consumption.

        Returns:
            Terminal tasks in queue order, or an empty list if none are available.
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
        Pending input is discarded. Caller cancellation propagates. A failed
        run's error is raised again after cleanup, even if already observed.

        Raises:
            AquteError: If the run failed with an engine error.
            AquteTooManyTasksFailedError: If the run reached its failure limit.
            ExceptionGroup: If a limiter, retry callback, or worker failed.
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
                self._next_task_id = 0
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

        if self._specific_errors_to_retry is not None and not isinstance(
            task.error, self._specific_errors_to_retry
        ):
            return False

        return task._remaining_tries > 0

    async def _put_task_to_result(self, task: AquteTask[TData, TResult]) -> None:
        self._finished_tasks_count += 1
        await self.result_queue.put(task)
        self._results_changed.set()

    async def __aenter__(self) -> Self:
        """Start the engine when entering `async with engine`.

        Returns:
            This engine, ready for manual submission and result consumption.
        """
        self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Stop the engine and await cleanup when leaving the async context.

        This cancels unfinished processing. Call `finish()` inside the context
        while consuming results if every admitted task must complete.
        Block exceptions are not suppressed. Cleanup can re-raise a run failure.

        Args:
            exc_type: Exception type from the context body, or `None`.
            exc: Exception instance from the context body, or `None`.
            tb: Traceback from the context body, or `None`.

        Raises:
            AquteError: If the run failed with an engine error.
            AquteTooManyTasksFailedError: If the run reached its failure limit.
            ExceptionGroup: If a limiter, retry callback, or worker failed.
        """
        await self.stop()
