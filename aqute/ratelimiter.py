import asyncio
import logging
from collections import deque
from time import perf_counter
from typing import Deque, Protocol, Union

logger = logging.getLogger("aqute.ratelimiter")


class RateLimiter(Protocol):
    async def acquire(self, name: str = "") -> None:
        """Shoild block inside this method if needed"""


class TokenBucketRateLimiter:
    def __init__(
        self,
        max_rate: int,
        time_period: Union[int, float] = 1,
        allow_burst: bool = False,
    ) -> None:
        """
        Initializes the `TokenBucketRateLimiter` with a given rate, time period,
        and burst requests permission.

        This rate limiter employs the token bucket technique where tokens are added
        to the bucket at a certain rate up to the bucket's capacity. An action consumes
        a token from the bucket.

        Args:
            max_rate: Maximum rate at which tokens are added to the bucket per second.
            time_period (optional): Period in seconds over which max_rate is measured.
                Defaults to 1 second.
            allow_burst (optional): allow to burst requests if bucket capacity is
                avaliable.
        """
        if max_rate < 1 or time_period <= 0:
            raise ValueError(
                f"Invalid values for configuration: {max_rate=}, {time_period=}"
            )
        self._fill_rate = max_rate / time_period
        self._bucket_capacity = max_rate if allow_burst else 1
        self._tokens = float(self._bucket_capacity)
        self._last_fill = perf_counter()
        self._lock = asyncio.Lock()

    def _refill_tokens(self) -> None:
        """
        Refills tokens in the bucket based on the elapsed time since the last refill.
        """
        current_time = perf_counter()
        time_elapsed = current_time - self._last_fill
        tokens_to_add = time_elapsed * self._fill_rate
        self._tokens = min(self._bucket_capacity, self._tokens + tokens_to_add)
        self._last_fill = current_time

    async def acquire(self, name: str = "") -> None:
        """
        Acquires permission to proceed based on the rate limits set.

        If the bucket is empty, it waits for the necessary duration for tokens to be
        refilled. Once there's at least one token, the action is allowed and a token
        is consumed.

        Args:
            name (optional): A label or identifier for the action. Primarily used
            for logging. Defaults to an empty string.
        """
        async with self._lock:
            while self._tokens < 1:
                self._refill_tokens()
                sleep_time = (1 - self._tokens) / self._fill_rate
                logger.debug(f"Got {sleep_time:.5f} <{name}>")
                await asyncio.sleep(sleep_time)
            self._tokens -= 1


class SlidingRateLimiter:
    def __init__(self, max_rate: int, time_period: Union[int, float] = 1):
        """
        Initializes the `SlidingRateLimiter` with a given rate over a time period.

        This rate limiter employs a sliding window technique to regulate the quantity
        of actions within the given time span.

        Args:
            max_rate: Maximum allowable actions within the time period.
            time_period (optional): Period in seconds for rate measurement. Defaults
            to 1 second.
        """
        if max_rate < 1 or time_period <= 0:
            raise ValueError(
                f"Invalid values for configuration: {max_rate=}, {time_period=}"
            )
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
