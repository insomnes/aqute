import asyncio
from dataclasses import dataclass
from typing import Generic, Optional, TypeVar

END_MARKER = object()


TData = TypeVar("TData")
TResult = TypeVar("TResult")


@dataclass
class AquteTask(Generic[TData, TResult]):
    data: TData
    task_id: str

    result: Optional[TResult] = None
    error: Optional[Exception] = None
    success: bool = False

    _remaining_tries: int = 0


AquteTaskQueueType = asyncio.Queue[AquteTask[TData, TResult]]
