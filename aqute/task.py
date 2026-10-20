import asyncio
from dataclasses import dataclass
from typing import Generic, NamedTuple, TypeVar, cast

from aqute.errors import AquteError

TData = TypeVar("TData")
TResult = TypeVar("TResult")


class AquteCounters(NamedTuple):
    """Immutable snapshot of counts for one engine run.

    Terminal counts can overlap with `running` while a worker publishes its
    result. Reading results does not change counts. `Aqute.stop()` resets them;
    retained results do not count toward a later run.
    """

    pending: int
    """Admitted tasks awaiting a worker; excludes submissions waiting for capacity."""

    running: int
    """Occupied workers, including limiter, retry-delay, and publication waits."""

    succeeded: int
    """Terminal successful handler outcomes, including those awaiting publication."""

    failed: int
    """Terminal failed handler outcomes after retries; excludes cancelled work."""

    retries: int
    """Additional handler invocations that started; excludes retries still waiting."""


@dataclass(eq=False, order=False)
class AquteTask(Generic[TData, TResult]):
    """Input data and outcome of one task.

    Engine APIs return terminal tasks after retry rules have been applied.
    A successful handler can return `None`; use `success` or `unwrap()` to
    distinguish success from a pending task. Direct `Foreman` results do not
    have the engine's `success` flag set.
    """

    data: TData
    """Input passed to the handler on each attempt."""

    task_id: str
    """Task identifier; caller-supplied IDs do not provide deduplication."""

    result: TResult | None = None
    """Handler return value on success; initially `None`, also a valid result."""

    error: Exception | None = None
    """Stored handler error on terminal failure; `None` when no error is stored."""

    success: bool = False
    """Whether the engine collected a successful terminal outcome; initially false."""

    _remaining_tries: int = 0
    _priority: int = 1_000_000

    def unwrap(self) -> TResult:
        """Return a successful terminal result, including None, or raise its error.

        Call this after receiving a completed task from `Aqute`.
        Calling `unwrap()` after `process_all()` does not make processing fail
        fast: the batch has already completed. Outcome fields are unchanged.

        Returns:
            The successful handler return value, which can be `None`.

        Raises:
            AquteError: If no error is stored and `success` is false, including
                pending tasks and successful tasks returned directly by `Foreman`.
            Exception: The stored task error, if present.
        """
        if self.error is not None:
            raise self.error
        if not self.success:
            raise AquteError(f"Task {self.task_id} is not complete")
        return cast(TResult, self.result)

    def __lt__(self, other: "AquteTask") -> bool:
        """Used for priority queue sorting"""

        return self._priority < other._priority


AquteTaskQueueType = asyncio.Queue[AquteTask[TData, TResult]]
