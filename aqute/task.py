import asyncio
from dataclasses import dataclass
from typing import Generic, NamedTuple, TypeVar

END_MARKER = object()


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

    def __lt__(self, other: "AquteTask") -> bool:
        """Used for priority queue sorting"""

        return self._priority < other._priority


AquteTaskQueueType = asyncio.Queue[AquteTask[TData, TResult]]
