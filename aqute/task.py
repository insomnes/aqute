import asyncio
from dataclasses import dataclass
from typing import Any, Optional

END_MARKER = object()


@dataclass
class AquteTask:
    data: Any
    task_id: str

    result: Optional[Any] = None
    error: Optional[Exception] = None
    success: bool = False

    _remaining_tries: int = 0


AquteTaskQueueType = asyncio.Queue[AquteTask]
