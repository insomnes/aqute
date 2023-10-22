import asyncio
import logging
from collections import deque
from time import perf_counter
from typing import Deque, Protocol, Union

logger = logging.getLogger("aqute.ratelimiter")


class RateLimiter(Protocol):
    async def acquire(self, name: str = "") -> None:
        """Shoild block inside this method if needed"""


class SlidingRateLimiter:
    def __init__(self, max_rate: int, time_period: Union[int, float] = 60):
        """
        Initializes the `SlidingRateLimiter` with a given rate over a time period.

        This rate limiter employs a sliding window technique to regulate the quantity
        of actions within the given time span.

        Args:
            max_rate: Maximum allowable actions within the time period.
            time_period (optional): Period in seconds for rate measurement. Defaults
            to 60 seconds.
        """
        self._max_rate = max_rate
        self._time_period = time_period
        self._timestamps: Deque = deque(maxlen=max_rate)
        self._lock = asyncio.Lock()

    async def acquire(self, name: str = "") -> None:
        """
        Acquires permission to proceed based on the rate limits set.

        If the action rate has reached its limit, it waits for the necessary
        duration to ensure the rate isn't exceeded. The current action timestamp
        is then recorded in the sliding window.

        Args:
            name (optional): A label or identifier for the action. Primarily used
            for logging. Defaults to an empty string.
        """
        async with self._lock:
            current_time = perf_counter()

            if len(self._timestamps) == self._max_rate:
                oldest_time = self._timestamps[0]
                sleep_time = self._time_period - (current_time - oldest_time)
                if sleep_time > 0:
                    logger.debug(f"Got {sleep_time:.5f} <{name}>")
                    await asyncio.sleep(sleep_time)

            self._timestamps.append(perf_counter())
