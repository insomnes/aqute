# Engine

Import the engine with `from aqute import Aqute`.
Use `process_all()` for an ordered batch or `iter_results()` for a managed stream.
For continuous input, use the manual lifecycle.

See the usage guide for [buffering](../usage.md#buffering),
[streaming cleanup](../usage.md#streaming-and-cleanup),
[retry and timeout behavior](../usage.md#errors-retries-and-timeouts), and
[manual shutdown](../usage.md#manual-processing-and-shutdown).
Queue annotations named `AquteTaskQueueType[TData, TResult]` mean
`asyncio.Queue[AquteTask[TData, TResult]]`.

::: aqute.Aqute
    options:
      members:
        - counters
        - process_all
        - iter_results
        - start
        - add_task
        - finish_submitting
        - finish
        - run
        - get_result
        - drain_results
        - stop
        - __aenter__
        - __aexit__
