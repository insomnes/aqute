import asyncio
from dataclasses import dataclass
from typing import Generic, TypeVar

END_MARKER = object()


TData = TypeVar("TData")
TResult = TypeVar("TResult")


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
