# Tasks and counters

Import these types with `from aqute import AquteTask, AquteCounters`.
Engine results are terminal tasks. See [task results](../usage.md#task-results)
for failure handling and [progress counters](../usage.md#progress-counters)
for snapshot timing.

Construct a task for the [worker queue API](worker-queues.md) with its input and ID:

```python
from aqute import AquteTask

task: AquteTask[str, int] = AquteTask(data="payload", task_id="job-1")
```

When using `Aqute`, submit input through `add_task()`, `process_all()`, or
`iter_results()`; the engine creates tasks and manages their retry and priority
state. The fields below describe the public task data.

::: aqute.AquteTask
    options:
      merge_init_into_class: false
      members:
        - data
        - task_id
        - result
        - error
        - success
        - unwrap

::: aqute.AquteCounters
    options:
      members:
        - pending
        - running
        - succeeded
        - failed
        - retries
