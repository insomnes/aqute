import asyncio
import logging
import math
import random
from collections import defaultdict, deque
from time import monotonic, perf_counter
from typing import Protocol

from aqute.task import AquteTask

logger = logging.getLogger("aqute.ratelimiter")


class RateLimiter(Protocol):
    """Protocol for asynchronous attempt admission.

    Implement `acquire()` on the application's event loop. Every retry acquires
    a permit again. Implementations must propagate cancellation and avoid
    blocking the event loop.
    """

    async def acquire(self, name: str = "", task: AquteTask | None = None) -> None:
        """Wait until one handler attempt may proceed.

        Args:
            name: Worker identifier; implementations may use it for partitioning
                or logging.
            task: Task awaiting admission, or `None` for a direct acquisition.

        Raises:
            asyncio.CancelledError: If the caller is cancelled while waiting.
        """


class PausableRateLimiter:
    """Add a shared monotonic pause to any RateLimiter on the same event loop.

    Pauses delay attempt admission, including retries, outside
    task_timeout_seconds. A throttled attempt still consumes retry_count.
    SDK-internal retries inside the handler multiply attempts without acquiring
    this limiter again. Requests already admitted are not interrupted.

    The inner limiter grants once per acquire(). A pause imposed during that
    grant can delay its delivery; delayed grants may resume together.
    """

    def __init__(self, inner: RateLimiter) -> None:
        """Wrap a limiter with a shared pause.

        Args:
            inner: Limiter to acquire once per call, between pause checks.
        """
        self._inner = inner
        self._paused_until = 0.0

    @property
    def paused_until(self) -> float:
        """Latest monotonic deadline, initially zero; retained after expiry.

        Returns:
            The absolute deadline in seconds from `time.monotonic()`.
        """
        return self._paused_until

    def pause_for(self, seconds: float) -> None:
        """Extend the pause by finite, nonnegative seconds from now.

        A shorter pause never reduces the existing deadline. Invalid values
        leave the pause unchanged.

        Args:
            seconds: Finite, nonnegative duration from `time.monotonic()` now.
                Zero adds no wait.

        Raises:
            ValueError: If `seconds` is negative or nonfinite.
        """
        if not math.isfinite(seconds) or seconds < 0:
            raise ValueError("seconds must be finite and nonnegative")
        self.pause_until(monotonic() + seconds)

    def pause_until(self, deadline: float) -> None:
        """Extend the pause to a time.monotonic() deadline.

        Past deadlines do not delay acquisition. A shorter deadline never
        reduces the current pause.

        Args:
            deadline: Finite, nonnegative absolute `time.monotonic()` time.

        Raises:
            ValueError: If `deadline` is negative or nonfinite.
        """
        if not math.isfinite(deadline) or deadline < 0:
            raise ValueError("deadline must be finite and nonnegative")
        self._paused_until = max(self._paused_until, deadline)

    async def _wait_for_pause(self) -> None:
        # Sleep until the deadline, then check for extensions; this is not polling.
        while (remaining := self._paused_until - monotonic()) > 0:  # noqa: ASYNC110
            await asyncio.sleep(remaining)

    async def acquire(self, name: str = "", task: AquteTask | None = None) -> None:
        """Wait for pauses before and after acquiring the inner limiter.

        Recheck extensions made during either wait. Delayed inner grants can
        resume together; this wrapper does not reacquire their permits.
        Inner limiter errors propagate.

        Args:
            name: Worker identifier passed unchanged to the inner limiter.
            task: Task passed unchanged to the inner limiter.

        Raises:
            asyncio.CancelledError: If the caller is cancelled while waiting.
        """
        await self._wait_for_pause()
        await self._inner.acquire(name=name, task=task)
        await self._wait_for_pause()


class TokenBucketRateLimiter:
    """Control a shared rate with a token bucket that starts full.

    Idle time refills the bucket up to its capacity. Each acquisition consumes
    one token. The default capacity is one; burst mode uses `max_rate` tokens.
    """

    def __init__(
        self,
        max_rate: int,
        time_period: int | float = 1,
        allow_burst: bool = False,
    ) -> None:
        """Configure the refill rate and bucket capacity.

        Args:
            max_rate: Tokens added per `time_period`. Must be at least one.
            time_period: Finite positive measurement period in seconds.
            allow_burst: If true, capacity is `max_rate` tokens. Otherwise,
                capacity is one token, spacing grants by `time_period / max_rate`.

        Raises:
            ValueError: If `max_rate` is below one or `time_period` is not finite
                and positive.
        """
        if max_rate < 1 or not 0 < time_period < math.inf:
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

    async def acquire(self, name: str = "", task: AquteTask | None = None) -> None:
        """Wait for and consume one token.

        Args:
            name: Worker identifier used for logging.
            task: Task awaiting admission; accepted for protocol compatibility.

        Raises:
            asyncio.CancelledError: If the caller is cancelled while waiting.
        """
        async with self._lock:
            self._refill_tokens()
            while self._tokens < 1:
                sleep_time = (1 - self._tokens) / self._fill_rate
                logger.debug(f"Got {sleep_time:.5f} <{name}>")
                await asyncio.sleep(sleep_time)
                self._refill_tokens()
            self._tokens -= 1


class SlidingRateLimiter:
    """Limit grants in a rolling time window.

    Up to `max_rate` permits can be granted together when the window is empty.
    """

    def __init__(self, max_rate: int, time_period: int | float = 1):
        """Configure the rolling grant limit.

        Args:
            max_rate: Maximum grants per rolling window. Must be at least one.
            time_period: Finite positive window duration in seconds.

        Raises:
            ValueError: If `max_rate` is below one or `time_period` is not finite
                and positive.
        """
        if max_rate < 1 or not 0 < time_period < math.inf:
            raise ValueError(
                f"Invalid values for configuration: {max_rate=}, {time_period=}"
            )
        self._max_rate = max_rate
        self._time_period = time_period
        self._timestamps: deque[float] = deque(maxlen=max_rate)
        self._lock = asyncio.Lock()

    async def acquire(self, name: str = "", task: AquteTask | None = None) -> None:
        """Wait for window capacity and record one grant.

        Args:
            name: Worker identifier used for logging.
            task: Task awaiting admission; accepted for protocol compatibility.

        Raises:
            asyncio.CancelledError: If the caller is cancelled while waiting.
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
    """Apply a separate token bucket to each worker name.

    Each bucket starts with one token and spaces subsequent grants independently.
    Aggregate throughput depends on the number of distinct names.
    """

    def __init__(self, max_rate: int, time_period: int | float = 1):
        """Configure the refill rate for each worker's bucket.

        Args:
            max_rate: Tokens added per worker per `time_period`. Must be at least one.
            time_period: Finite positive measurement period in seconds.

        Raises:
            ValueError: If `max_rate` is below one or `time_period` is not finite
                and positive.
        """
        if max_rate < 1 or not 0 < time_period < math.inf:
            raise ValueError(
                f"Invalid values for configuration: {max_rate=}, {time_period=}"
            )

        self._per_worker_limiters: dict[str, TokenBucketRateLimiter] = defaultdict(
            lambda: TokenBucketRateLimiter(max_rate, time_period)
        )

    async def acquire(self, name: str = "", task: AquteTask | None = None) -> None:
        """Wait for and consume a token from the named worker's bucket.

        Args:
            name: Bucket key. Calls with the same name share a rate limit,
                including calls that omit the name.
            task: Task awaiting admission; accepted for protocol compatibility.

        Raises:
            asyncio.CancelledError: If the caller is cancelled while waiting.
        """

        await self._per_worker_limiters[name].acquire(name)


class RandomizedIntervalRateLimiter:
    """Limit rolling grants and add bounded random delay to every acquisition.

    Extra delay applies at startup and after idle. Quota and scheduler waits
    can exceed the jitter bounds and reduce throughput below the configured rate.
    """

    def __init__(
        self,
        max_rate: int,
        time_period: int | float = 1,
        mean_target_multiplier: float = 0.9,
        std_dev: float = 0.2,
        lower_multiplier_bound: float = 0.0,
        upper_multiplier_bound: float = 1.1,
        lower_upper_fluctuation: float = 0.005,
    ):
        """Configure the rolling grant limit and randomized delay.

        Each grant waits for quota, then for an additional randomized delay,
        including at startup and after idle. Independent random phases reduce
        counter-linked regularity; they do not model human behavior or guarantee
        detection avoidance. Additional waits can reduce sustained throughput.

        Args:
            max_rate: Maximum grants per rolling window. Must be at least one.
            time_period: Finite positive window duration in seconds.
            mean_target_multiplier: Mean of the Gaussian input, before
                phase scaling and bounding. This is not the mean emitted delay.
            std_dev: Standard deviation of the Gaussian input, before scaling
                and bounding.
            lower_multiplier_bound: Nonnegative lower multiplier bound,
                ensuring a minimum additional delay.
            upper_multiplier_bound: Upper multiplier bound for the additional
                delay, excluding quota and scheduler waits.
            lower_upper_fluctuation: Inward bound adjustment, multiplied
                by the same absolute sine of a uniform phase as the Gaussian input.
                Use nonnegative bounds and fluctuation with twice the fluctuation
                no greater than the difference between upper and lower bounds.
                Multipliers convert to seconds through `time_period / max_rate`.
                The caller must ensure these jitter constraints; only rate and
                period are validated.

        Raises:
            ValueError: If `max_rate` is below one or `time_period` is not finite
                and positive.
        """
        if max_rate < 1 or not 0 < time_period < math.inf:
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

        self._request_times: deque[float] = deque(maxlen=max_rate)
        self._lock = asyncio.Lock()

    def _get_multiplier(self) -> float:
        """
        Samples one phase for the Gaussian amplitude and both effective bounds.
        """
        value = random.gauss(self._mean_mul, self._std_dev)
        oscillation = abs(math.sin(random.uniform(0.0, math.tau)))
        value = value * oscillation

        fluctuation = oscillation * self._fluctuation
        max_value = min(self._upper_bound - fluctuation, value)

        return max(self._lower_bound + fluctuation, max_value)

    def _get_sleep(self) -> float:
        return self._optimal_sleep * self._get_multiplier()

    async def acquire(self, name: str = "", task: AquteTask | None = None) -> None:
        """Wait for quota, then the full random delay, and record one grant.

        Args:
            name: Worker identifier; accepted for protocol compatibility.
            task: Task awaiting admission; accepted for protocol compatibility.

        Raises:
            asyncio.CancelledError: If the caller is cancelled while waiting.
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
