import asyncio
import logging
import math
import random
from collections import defaultdict, deque
from time import perf_counter
from typing import Optional, Protocol, Union

from aqute.task import AquteTask

logger = logging.getLogger("aqute.ratelimiter")


class RateLimiter(Protocol):
    async def acquire(self, name: str = "", task: Optional[AquteTask] = None) -> None:
        """Should block inside this method if needed"""


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
                available.
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

    async def acquire(self, name: str = "", task: Optional[AquteTask] = None) -> None:
        """
        Acquires permission to proceed based on the rate limits set.

        If the bucket is empty, it waits for the necessary duration for tokens to be
        refilled. Once there's at least one token, the action is allowed and a token
        is consumed.

        Args:
            name (optional): A label or identifier for the action. Primarily used
                for logging. Defaults to an empty string.
            task (optional): The task that is attempting to acquire a rate limit token.
                Defaults to None for protocol compatibility.
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
        self._timestamps: deque[float] = deque(maxlen=max_rate)
        self._lock = asyncio.Lock()

    async def acquire(self, name: str = "", task: Optional[AquteTask] = None) -> None:
        """
        Acquires permission to proceed based on the rate limits set.

        If the action rate has reached its limit, it waits for the necessary
        duration to ensure the rate isn't exceeded. The current action timestamp
        is recorded in the sliding window to ensure rate limiting compliance.

        Args:
            name (optional): A label or identifier for the action. Primarily used
                for logging. Defaults to an empty string.
            task (optional): The task that is attempting to acquire a rate limit token.
                Defaults to None for protocol compatibility.
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


class PerWorkerRateLimiter:
    def __init__(self, max_rate: int, time_period: Union[int, float] = 1):
        """
        Initializes the `PerWorkerRateLimiter` with a specified rate and time period
        for each unique worker.

        This rate limiter maintains separate instances of `TokenBucketRateLimiter`
        for each worker, identified by a unique name.
        It ensures that rate limiting is enforced independently for each worker.

        Args:
            max_rate: Maximum allowable actions within the time period.
            time_period (optional): Period in seconds for rate measurement. Defaults
                to 1 second.
        """
        if max_rate < 1 or time_period <= 0:
            raise ValueError(
                f"Invalid values for configuration: {max_rate=}, {time_period=}"
            )

        self._per_worker_limiters: dict[str, TokenBucketRateLimiter] = defaultdict(
            lambda: TokenBucketRateLimiter(max_rate, time_period)
        )

    async def acquire(self, name: str = "", task: Optional[AquteTask] = None) -> None:
        """
        Acquires a token for a particular worker, identified by `name`,
        to proceed with an action.

        If the limit for the worker has been reached, this method will block
        until a token becomes available, ensuring that rate limits are not exceeded.

        Args:
            name (optional): The identifier for the worker that is attempting to
                acquire a rate limit token. Defaults to an empty string
                for protocol compatibility.
            task (optional): The task that is attempting to acquire a rate limit token.
                Defaults to None for protocol compatibility.
        """

        await self._per_worker_limiters[name].acquire(name)


class RandomizedIntervalRateLimiter:
    def __init__(
        self,
        max_rate: int,
        time_period: Union[int, float] = 1,
        mean_target_multiplier: float = 0.9,
        std_dev: float = 0.2,
        lower_multiplier_bound: float = 0.0,
        upper_multiplier_bound: float = 1.1,
        lower_upper_fluctuation: float = 0.005,
    ):
        """
        Initializes the RandomizedIntervalRateLimiter with specified parameters,
        defining the rate limiting behavior with randomized intervals.
        Sleep interval duratation is randomized, but rate limiting is assured for
        max_rate in time_period. Can be used to emulate more "human-like" behavior.

        Args:
            max_rate: Maximum allowable actions within the time period.
            time_period (optional): Period in seconds for rate measurement. Defaults
                to 1 second.
            mean_target_multiplier (optional): The mean multiplier influencing
                the average sleep duration.
            std_dev (optional): The standard deviation for the Gaussian distribution,
                adding randomness to the multiplier.
            lower_multiplier_bound (optional): The lower bound for the multiplier,
                ensuring a minimum sleep duration.
            upper_multiplier_bound (optional): The upper bound for the multiplier,
                ensuring a maximum sleep duration.
            lower_upper_fluctuation (optional): The fluctuation margin for the
                multiplier bounds, adding variability when bounds are reached.
        """
        if max_rate < 1 or time_period <= 0:
            raise ValueError(
                f"Invalid values for configuration: {max_rate=}, {time_period=}"
            )
        self._max_rate = max_rate
        self._time_period = time_period

        self._optimal_sleep = time_period / max_rate

        self._mean_mul = mean_target_multiplier
        self._std_dev = std_dev
        self._lower_bound = lower_multiplier_bound
        self._upper_bound = upper_multiplier_bound
        self._fluctuation = lower_upper_fluctuation
        self._gen_count = 0

        self._request_times: deque[float] = deque(maxlen=max_rate)
        self._lock = asyncio.Lock()

    def _get_multiplier(self) -> float:
        """
        Determines the sleep time multiplier. The multiplier is based on a Gaussian
        distribution and sinusoidal oscillation, ensuring variability within the
        predefined bounds. This method is a key component in managing the randomized
        intervals between actions.
        """
        value = random.gauss(self._mean_mul, self._std_dev)
        oscillation = abs(math.sin(self._gen_count))
        value = value * oscillation
        self._gen_count += 1

        fluctuation = oscillation * self._fluctuation
        max_value = min(self._upper_bound - fluctuation, value)

        return max(self._lower_bound + fluctuation, max_value)

    def _get_sleep(self) -> float:
        return self._optimal_sleep * self._get_multiplier()

    async def acquire(self, name: str = "", task: Optional[AquteTask] = None) -> None:
        """
        Acquires permission to proceed based on the rate limits set.

        If the action rate has reached its limit, it waits for the necessary
        duration to ensure the rate isn't exceeded. Sleeps for a randomized interval
        after this check.
        The current action timestamp is then recorded for the next
        max-rate'th limit check.

        Args:
            name (optional): A label or identifier for the action. Primarily used
                for logging. Defaults to an empty string.
            task (optional): The task that is attempting to acquire a rate limit token.
                Defaults to None for protocol compatibility.
        """
        async with self._lock:
            current_time = perf_counter()
            if len(self._request_times) >= self._max_rate:
                elapsed_since_oldest = current_time - self._request_times[0]

                if elapsed_since_oldest < self._time_period:
                    sleep_time = self._time_period - elapsed_since_oldest
                    await asyncio.sleep(sleep_time)

            extra_sleep = self._get_sleep()
            await asyncio.sleep(extra_sleep)

            self._request_times.append(perf_counter())
