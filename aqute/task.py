import asyncio
from dataclasses import dataclass
from typing import Generic, NamedTuple, TypeVar, cast

from aqute.errors import AquteError

TData = TypeVar("TData")
TResult = TypeVar("TResult")


class AquteCounters(NamedTuple):
    """Immutable current-run counts; see Aqute.counters for their meanings."""

    pending: int
    running: int
    succeeded: int
    failed: int
    retries: int


@dataclass(eq=False, order=False)
class AquteTask(Generic[TData, TResult]):
    data: TData
    task_id: str

    result: TResult | None = None
    error: Exception | None = None
    success: bool = False

    _remaining_tries: int = 0
    _priority: int = 1_000_000

    def unwrap(self) -> TResult:
        """Return a successful terminal result, including None, or raise its error.

        Call this after receiving a completed task. A pending task raises AquteError.
        Calling unwrap after process_all does not make processing fail fast:
        the batch has already completed.
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
