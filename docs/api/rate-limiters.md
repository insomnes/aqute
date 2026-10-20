# Rate limiters

Import limiters from `aqute.ratelimiter`. Each handler attempt, including a retry,
acquires the configured limiter. Limiter waits occupy a worker and are outside
the handler timeout.

See [rate-limit behavior](../reference.md#rate-limiters),
[shared pauses](../reference.md#shared-pauses),
[randomized intervals](../reference.md#randomized-intervals), and
[custom limiters](../reference.md#custom-limiters).

::: aqute.ratelimiter.RateLimiter
    options:
      members:
        - acquire

::: aqute.ratelimiter.TokenBucketRateLimiter
    options:
      members:
        - acquire

::: aqute.ratelimiter.SlidingRateLimiter
    options:
      members:
        - acquire

::: aqute.ratelimiter.PerWorkerRateLimiter
    options:
      members:
        - acquire

::: aqute.ratelimiter.RandomizedIntervalRateLimiter
    options:
      members:
        - acquire

::: aqute.ratelimiter.PausableRateLimiter
    options:
      members:
        - paused_until
        - pause_for
        - pause_until
        - acquire
