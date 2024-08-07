import asyncio
import logging
from collections.abc import Coroutine
from typing import Any, Callable, Generic, Optional, Union

from aqute.errors import AquteTaskTimeoutError
from aqute.ratelimiter import RateLimiter
from aqute.task import END_MARKER, AquteTask, AquteTaskQueueType, TData, TResult

logger = logging.getLogger("aqute.worker")


class Worker(Generic[TData, TResult]):
    def __init__(
        self,
        name: str,
        handle_coro: Callable[[TData], Coroutine[Any, Any, TResult]],
        input_q: AquteTaskQueueType[TData, TResult],
        output_q: AquteTaskQueueType[TData, TResult],
        rate_limiter: Optional[RateLimiter] = None,
        task_timeout_seconds: Optional[Union[int, float]] = None,
    ):
        self.handle_coro = handle_coro
        self.input_q = input_q
        self.output_q = output_q
        self.name = name

        self.rate_limiter = rate_limiter
        self.task_timeout_seconds = task_timeout_seconds

    async def run(self) -> None:
        while True:
            task = await self.input_q.get()
            logger.debug(f"Worker {self.name} got task {task.task_id}")

            if task.data is END_MARKER:
                break

            await self.handle_task(task)

    async def handle_task(self, task: AquteTask[TData, TResult]) -> None:
        if self.rate_limiter:
            await self.rate_limiter.acquire(name=self.name, task=task)
        try:
            task.result = await asyncio.wait_for(
                self.handle_coro(task.data), timeout=self.task_timeout_seconds
            )
        except asyncio.TimeoutError:
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
        await self.output_q.put(task)


class Foreman(Generic[TData, TResult]):
    def __init__(
        self,
        handle_coro: Callable[[TData], Coroutine[Any, Any, TResult]],
        workers_count: int,
        rate_limiter: Optional[RateLimiter] = None,
        input_task_queue_size: int = 0,
        use_priority_queue: bool = False,
        task_timeout_seconds: Optional[Union[int, float]] = None,
    ):
        """
        Initializes a Worker instance to process tasks.

        Args:
            name: Identifier for the worker.
            handle_coro: Coroutine designated for task processing.
            input_q: Queue from which tasks are fetched.
            output_q: Queue to put processed tasks into.
            rate_limiter (optional): Tool to control processing rate. If not given,
                processing won't be rate-limited.
            input_task_queue_size (optional): Maximum size of the input queue. Defaults
                to 0, which means no limit.
            use_priority_queue (optional): Whether to use a priority queue for input.
                Defaults to False.
            task_timeout_seconds (optional): Timeout for task handler coroutine wait.
                Defaults to None. AquteTaskTimeoutError will be raised
                if task processing takes longer than this value.
        """
        self._handle_coro = handle_coro
        self._workers_count = workers_count
        self._rate_limiter = rate_limiter

        self._input_task_queue_size = input_task_queue_size
        self._use_priority_queue = use_priority_queue

        self._task_timeout_seconds = task_timeout_seconds

        self.in_queue: AquteTaskQueueType[TData, TResult] = self._create_task_queue(
            input_task_queue_size
        )
        self.out_queue: AquteTaskQueueType[TData, TResult] = asyncio.Queue()
        self._workers: list[Worker[TData, TResult]] = []

        self._worker_jobs: list[asyncio.Task] = []
        self.reset()

    def start(self) -> None:
        """
        Initiates the worker processes.

        If the workers haven't been initialized yet, they'll be
        set to start processing tasks.
        """
        if not self._worker_jobs:
            self._start_workers()

    async def add_task(self, task: AquteTask[TData, TResult]) -> None:
        """
        Adds a specified task to the input queue for processing.

        Args:
            task: The task to be processed.
        """
        await self.in_queue.put(task)

    async def get_handled_task(self) -> AquteTask[TData, TResult]:
        """
        Retrieves a processed task from the worker's output queue.

        Returns:
            AquteTask: The processed task from the queue.
        """
        return await self.out_queue.get()

    async def finalize(self) -> None:
        """
        Asynchronously finalize the current operations.

        This method ensures that ending tasks are added, waits for
        workers to complete their tasks, and then stops the workers.
        """
        await self._add_ending_tasks()
        await self._wait_for_workers()
        await self.stop()

    async def stop(self) -> None:
        """
        Asynchronously stops all active workers.

        It checks if there are active worker jobs, logs the stopping action,
        cancels all active worker jobs,
        awaits for the jobs to stop with a 2-second timeout,
        logs once all workers have stopped, and then resets the Foreman
        to its initial state.
        """
        if not self._worker_jobs:
            return
        logger.debug("Stopping workers")
        for t in self._worker_jobs:
            t.cancel()
        await asyncio.gather(
            *[asyncio.wait_for(t, timeout=2) for t in self._worker_jobs],
            return_exceptions=True,
        )
        logger.debug("Workers stopped")
        self.reset()

    def reset(self) -> None:
        """
        Resets the Foreman state.

        Via re-initializing worker input and output queues,
        re-creating the worker instances, and clearing
        the list of worker jobs. Also, logs the resetting action.
        """
        logger.debug("Resetting workers")
        self.in_queue = self._create_task_queue(size=self._input_task_queue_size)
        self.out_queue = asyncio.Queue()
        self._workers = [
            Worker(
                name=f"worker_{i}",
                handle_coro=self._handle_coro,
                input_q=self.in_queue,
                output_q=self.out_queue,
                rate_limiter=self._rate_limiter,
                task_timeout_seconds=self._task_timeout_seconds,
            )
            for i in range(self._workers_count)
        ]
        self._worker_jobs = []

    def _start_workers(self) -> None:
        if self._worker_jobs:
            logger.debug("Workers already started, skipping")
            return
        self._worker_jobs = [asyncio.create_task(w.run()) for w in self._workers]
        logger.debug(f"Started {len(self._worker_jobs)} workers")

    async def _add_ending_tasks(self) -> None:
        for i, wj in enumerate(self._worker_jobs):
            if wj.done():
                logger.debug(f"Worker {i} already done, skipping adding end job")
                continue
            await self.in_queue.put(
                AquteTask(
                    data=END_MARKER,  # type: ignore
                    task_id=f"Finish_{i}",
                    _remaining_tries=1,
                )
            )

        logger.debug("Added ending tasks")

    async def _wait_for_workers(self) -> None:
        logger.debug("Waiting for workers to finish")
        await asyncio.gather(*self._worker_jobs)

    def _create_task_queue(self, size: int = 0) -> AquteTaskQueueType[TData, TResult]:
        if self._use_priority_queue:
            return asyncio.PriorityQueue(maxsize=size)
        return asyncio.Queue(maxsize=size)
