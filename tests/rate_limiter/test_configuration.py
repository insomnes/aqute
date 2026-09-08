import pytest

from aqute.ratelimiter import (
    PerWorkerRateLimiter,
    RandomizedIntervalRateLimiter,
    SlidingRateLimiter,
    TokenBucketRateLimiter,
)


@pytest.mark.parametrize(
    "limiter_class",
    [
        TokenBucketRateLimiter,
        SlidingRateLimiter,
        PerWorkerRateLimiter,
        RandomizedIntervalRateLimiter,
    ],
)
@pytest.mark.parametrize(
    "time_period", [0, -1, float("nan"), float("inf"), -float("inf")]
)
def test_invalid_time_period(limiter_class, time_period):
    """Reject invalid periods before the limiter can accept work."""
    with pytest.raises(ValueError):
        limiter_class(1, time_period)
