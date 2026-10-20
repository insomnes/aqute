# Exceptions

These exceptions are exported from `aqute`.
Handler failures normally appear in [`AquteTask.error`](tasks.md#aqute.AquteTask.error).
`unwrap()` raises the stored error. Run failures propagate through the engine
lifecycle. See [errors, retries, and timeouts](../usage.md#errors-retries-and-timeouts).

::: aqute.AquteError
    options:
      members: []

::: aqute.AquteTaskTimeoutError
    options:
      members: []

::: aqute.AquteTooManyTasksFailedError
    options:
      members: []
