class AquteError(Exception):
    """Base exception for Aqute lifecycle and task-state errors.

    Raised for invalid engine state, unavailable results, first-input timeout,
    or unexpected worker cancellation. Worker failures can wrap it in an
    `ExceptionGroup`.
    """


class AquteTaskTimeoutError(AquteError):
    """Handler attempt exceeded Aqute's configured timeout.

    Stored in `AquteTask.error` after retry rules are exhausted. A nonpositive
    timeout produces this error without invoking the handler. Limiter and
    retry-delay waits are excluded. A handler-raised `TimeoutError` keeps its
    original type when the Aqute deadline has not expired.
    """


class AquteTooManyTasksFailedError(AquteError):
    """Collected terminal failures reached `total_failed_tasks_limit`.

    The limit is inclusive. The engine publishes the triggering task result
    before failing the run and cancelling unfinished work. Lifecycle calls
    propagate this error; `stop()` can raise it again after cleanup.
    """
