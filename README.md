# Aqute
**A**sync **QU**eue **T**ask **E**ngine

Aqute is a minimalist yet potent Python library specifically designed 
for hassle-free asynchronous task processing. Leveraging the power of async programming,
Aqute offers:

 - **Efficient Producer-Consumer Model**: Utilizing the Producer-Consumer pattern with
 multiple workers, Aqute ensures streamlined task distribution and swift concurrent processing.
- **Worker count & Rate Limiting**: Regulate the execution rate of tasks and configure 
the number of workers for concurrent processing, ensuring optimal resource utilization.
You can use pre-programmed rate limiters or provide your own rate limiting implementation.
- **Resilient Retry Mechanism**: Tasks that encounter errors can automatically retry,
with options to specify which error types should trigger retries.
Exception in handler is returned as error-value.
- **Versatile task adding**: You can process the whole batch or add tasks on the fly,
depending on your needs.
- **Lightweight and simple**: Aqute operates efficiently without relying on any
external dependencies, ensuring seamless integration and minimal footprint in your projects.

Aqute simplifies task management in asynchronous landscapes, allowing developers
to focus on the task logic rather than concurrency challenges.

## Table of Contents
- [Install](#install)
- [Quickstart](#quickstart)
- [How to use it?](#how-to-use-it)
  - [Simple batch processing](#simple-batch-processing)
  - [Result as async generator per completed](#result-as-async-generator-per-completed)
  - [Infinite loop](#infinite-loop)
  - [Rate limiting](#rate-limiting)
  - [Manual task adding, context manager and error retry](#manual-task-adding-context-manager-and-error-retry)
  - [Manual flow management and custom result queue](#manual-flow-management-and-custom-result-queue)
  - [Even more manual management and internal worker queue size](#even-more-manual-management-and-internal-worker-queue-size)
  - [Use priroty queue](#use-priroty-queue)
  - [Task timeout setting](#task-timeout-setting)
  - [Early stopping on too many failed tasks](#early-stopping-on-too-many-failed-tasks)
  - [Barebone queue via Foreman](#barebone-queue-via-foreman)
- [Some caveats](#some-caveats)
  - [Start load timeout](#start-load-timeout)
  - [You can't wait on not started Aqute](#you-can-t-wait-on-not-started-aqute)
- [Misc](#misc)
  - [Instance reuse after stop()](#instance-reuse-after-stop)
  - [Type checking and generics](#type-checking-and-generics)

# Install
Python 3.9+ required:
```bash
pip install aqute
```

# Quickstart
Apply your async function to each item of some iterable and get list of wrapped in
`AquteTask` results, ordered the same way:

```python
import asyncio
from aqute import Aqute

async def main():
    async def handler(i: int) -> int:
        await asyncio.sleep(i / 20)
        return i * 2

    aqute = Aqute(handle_coro=handler, workers_count=2)
    result = await aqute.apply_to_all(range(10))
    # Do not forget to extract result data from wrapper object with <result> property
    assert [t.result for t in result] == [i * 2 for i in range(10)]

asyncio.run(main())
```

# How to use it?
While a deep dive is available through Aqute's method docstrings, it's not necessary.

Aqute is easy to use for both simple and advanced workflows.

## Simple batch processing
The easiest way to use Aqute is `apply_to_all()` method:

```python
import asyncio
import logging
from random import randint, random

from aqute import Aqute, AquteError


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

logger = logging.getLogger("main")

async def handler(i: int) -> str:
    """
    NOTE: This is a mock handler for demonstration purposes.
    Replace the logic below with your own specific task processing logic.
    """
    await asyncio.sleep(0.01 + (0.09) * random())
    
    # Guaranties failures for some tasks of examples
    if i >= 19:
        raise KeyError(f"The Key for {i}")
    
    # Here we have some randomness, so you can see retry after errors in play
    r = randint(1, 101)
    if r >= 80:
        raise ValueError(f"Got big number for {i}")

    return f"success {i}"

async def main():
    # Getting started example, the most simple
    input_data = list(range(20))

    # This will apply handler to every item of iterable and return result as list
    # with task results ordered as input iterable
    aqute = Aqute(handle_coro=handler, workers_count=10, retry_count=2)
    result = await aqute.apply_to_all(input_data)
    # Each task result is wrapped in AquteTask instance
    assert [t.data for t in result] == input_data

    ...


asyncio.run(main())

```

## Result as async generator per completed
```python
    # Like previous but the result is async generator and the tasks are yielded
    # in completion order
    input_data = list(range(20))
    aqute = Aqute(handle_coro=handler, workers_count=10)

    done, with_errors = [], []
    # You can determine final task status with specific success field
    async for task in aqute.apply_to_each(input_data):
        if task.success:
            done.append(task)
        else:
            with_errors.append(task)

    assert len(done + with_errors) == len(input_data)

```

## Infinite loop
You can run aqute on "infinite" amount of tasks if needed and control "end"
from outside. Outside of context or with `stop()` coro it will shutdown gracefully.

```python
import asyncio
import logging
from random import random

from aqute import Aqute


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

logger = logging.getLogger("main")

async def handler(i: int) -> str:
    """
    NOTE: This is a mock handler for demonstration purposes.
    """
    await asyncio.sleep(0.01 + (0.009) * random())

    return f"success {i}"

async def main():
    """
    This example shows "infinite"workflow for aqute.
    Here limited by 10_000 tasks, but it can be without any end.
    """

    # Only to show ruling the done / not done status from outside
    TASK_LIMIT = 10_000
    done = asyncio.Event()

    aqute = Aqute(handle_coro=handler, workers_count=20)

    async def add_tasks():
        # You can add tasks externally
        for i in range(TASK_LIMIT):
            await aqute.add_task(i)
            # Sleep to simulate some delay between tasks adding (req, calc, etc.)
            await asyncio.sleep(0.01 + (0.009) * random())

    result = []

    async def collect_results():
        # You can collect and handle results externally too
        while len(result) < TASK_LIMIT:
            task = await aqute.get_task_result()
            result.append(task)
        done.set()

    async with aqute:
        asyncio.create_task(add_tasks())
        asyncio.create_task(collect_results())
        # Better to use await done.wait() instead of while loop,
        # but it's just for demonstration purposes
        while not done.is_set():
            logger.info(f"Done tasks: {len(result):_}/{TASK_LIMIT:_}")
            await asyncio.sleep(5)

    logger.info(f"Done tasks: {len(result):_}/{TASK_LIMIT:_}")


asyncio.run(main())
```

## Rate limiting
You can also add RateLimiter instance to Aqute for rate limiting:
```python

    from aqute.ratelimiter import TokenBucketRateLimiter

    # Applied rate limiter with 5 handler calls per second.
    input_data = list(range(20))
    r_limit = TokenBucketRateLimiter(5, 1)
    aqute = Aqute(handle_coro=handler, workers_count=10, rate_limiter=r_limit)
    result = []
    async for task in aqute.apply_to_each(input_data):
        result.append(task)

    assert len(result) == len(input_data)
```

There are three available `RateLimiter` implementations:
- `TokenBucketRateLimiter`: steady rate by default, burstable with `allow_burst` option;
- `SlidingRateLimiter`: next call will be available after enough time from the oldest one;
- `PerWorkerRateLimiter`: enforces separate rate limits for each unique worker with separate `TokenBucketRateLimiter` instances;
- `RandomizedIntervalRateLimiter`: introduces more random intervals between each call,
but enforcing `max_rate` over `time_period` limit.

You can write your own `RateLimiter` implementation with specific algorithm if needed.

## Manual task adding, context manager and error retry
This can be most useful if not all of your tasks are available at the start:
```python
    # You can add tasks manually and also start/stop aqute with context
    # manager. And even add tasks on the fly.
    # Aqute is reliable for errors retry, you can specify your own
    # retry count (0 is used for no retries by default) and specify errors to retry or not
    # to keep retrying on all errors
    aqute = Aqute(
        handle_coro=handler,
        workers_count=10,
        # We we will retry 5 more times after first fail
        retry_count=5,
        # We retry only ValueError here
        specific_errors_to_retry=(ValueError,),
        # Either you can set this option to not retry only on ValueError instead
        errors_to_not_retry=(ValueError,),
    )
    for i in range(10):
        # You also can use your own task id for identification
        await aqute.add_task(i, task_id=f"My task id: {i}")


    async with aqute:
        await asyncio.sleep(0.1)
        for i in range(10, 15):
            await aqute.add_task(i, task_id=f"My task id: {i}")

        await asyncio.sleep(0.1)
        for i in range(15, 20):
            await aqute.add_task(i, task_id=f"My task id: {i}")

        # Set waiting for finalization when you have all tasks added
        await aqute.wait_till_end()

    # You can simply extract all results from queue with this method if aqute has 
    # finished, returns the list of AquteTask
    for tr in aqute.extract_all_results():
        logger.info(f"{tr.success, tr.error, tr.result}")
```

## Manual flow management and custom result queue
```python
    # You can manage the whole workflow manually if needed and use your own
    # result queue instance (with limited size for example)
    result_q = asyncio.Queue(5)

    aqute = Aqute(handle_coro=handler, workers_count=10, result_queue=result_q)
    for i in range(10):
        # We can't await here cause we will hang without queue emptying
        asyncio.create_task(aqute.add_task(i))
    await asyncio.sleep(0.1)

    # Starting the processing
    aqute.start()
    # Sleep enough for possibly all task to finish
    await asyncio.sleep(1)

    # We can see our result sizing works
    assert result_q.qsize() == 5
    for _ in range(5):
        await result_q.get()

    # Now wait till all finished via specific method, this also notifies
    # aqute that we have added all tasks
    await aqute.wait_till_end()
    assert result_q.qsize() == 5
    # Stop the aqute
    await aqute.stop()
```

## Even more manual management and internal worker queue size
```python
    # You can configure internal queue size for consumers if you want it to be limited
    aqute = Aqute(
        handle_coro=handler, workers_count=10, input_task_queue_size=2
    )
    for i in range(10):
        await aqute.add_task(i)
    # Should set it before awaiting bare start() if we want
    aqute.set_all_tasks_added()

    aqute_run_aiotask = aqute.start()
    await aqute_run_aiotask
    await aqute.stop()

    assert aqute.result_queue.qsize() == 10
```

## Use priroty queue
You can prioritize tasks by setting `use_priority_queue` flag:

```python
    async def handler(i: int) -> int:
        return i

    # Set flag for prioritezed queue, default task priority is 1_000_000
    aqute = Aqute(
        workers_count=1,
        handle_coro=handler,
        use_priority_queue=True,
    )
    await aqute.add_task(1_000_000)
    await aqute.add_task(10, task_priority=10)
    await aqute.add_task(5, task_priority=5)
    await aqute.add_task(10, task_priority=10)
    await aqute.add_task(1, task_priority=1)

    async with aqute:
        await aqute.wait_till_end()

    results = aqute.extract_all_results()
    assert [t.data for t in results] == [1, 5, 10, 10, 1_000_000]
```

## Task timeout setting
The `task_timeout_seconds` option is used to specify a time limit for each task.
If a task exceeds this duration, it is considered a timeout and is
handled according to the specified retry logic.

### Timeout with retries
By default setting timeout will result in task timeout if task exceeds value, but
retry logic is applied if `retry_count > 0`.

```python
aqute = Aqute(
    handle_coro=your_handler_coroutine,
    task_timeout_seconds=5,
    retry_count=2,
    # other parameters
)
```
 
### Do not retry on timeout
To disable this behavior you can set `errors_to_not_retry` with `AquteTaskTimeoutError`:
```python
aqute = Aqute(
    handle_coro=your_handler_coroutine,
    task_timeout_seconds=5,
    retry_count=2,
    errors_to_not_retry=AquteTaskTimeoutError,
    # other parameters
)
```

## Early stopping on too many failed tasks
If you want to stop the processing when too many tasks have failed, you can use the
`total_failed_tasks_limit` option. This will raise `AquteTooManyTasksFailedError` if
the limit is reached before all tasks are processed:
```python
async def failing_handler(task: int) -> int:
    await asyncio.sleep(0.01)
    if task % 2 == 0:
        raise ValueError("Even task number")
    return task

aqute = Aqute(
    workers_count=2,
    handle_coro=failing_handler,
    total_failed_tasks_limit=5,
)
for i in range(10):
    await aqute.add_task(i)

# This will raise AquteTooManyTasksFailedError cause we have enough failed tasks
# before all tasks are processed
async with aqute:
    await aqute.wait_till_end()
```

## Barebone queue via Foreman
If you don't need retry flow and high-level helpers you can use `Foreman` for bare flow,
but still with rate limiting support:
```python
import asyncio
from random import random

from aqute.worker import Foreman
from aqute.ratelimiter import TokenBucketRateLimiter

async def handler(i: int) -> str:
    await asyncio.sleep(0.01 + (0.09) * random())
    return f"Success {i}"

async def main():
    # These are the supported options for Foreman
    foreman = Foreman(
        handle_coro=handler,
        workers_count=10,
        rate_limiter=TokenBucketRateLimiter(5, 1),
        input_task_queue_size=100,
    )
    for i in range(20):
        await foreman.add_task(AquteTask(i, f"{i}"))

    foreman.start()

    result = []
    for _ in range(20):
        # Be aware that status and retries are not relevant here
        # But you can check the error field of output
        r = await foreman.get_handled_task()
        assert r.error is None
        logger.info(r.result)
        result.append(r)

    # Do not finalize before result extraction
    await foreman.finalize()
```


# Some caveats
## Start load timeout
If no tasks will be provided, and you've set the timeout, Aqute will intentionally fail:
```python
    try:
        async with Aqute(
            handle_coro=handler,
            workers_count=10,
            start_timeout_seconds=1,
        ) as aqute:
            await asyncio.sleep(1.2)
    except AquteError as exc:
        logger.error(f"Aqute timeouted: {exc}")
```

## You can't wait on not started Aqute
```python
    # 
    aqute = Aqute(handle_coro=handler, workers_count=10)

    try:
        await aqute.wait_till_end()
    except AquteError as exc:
        logger.error(f"Aqute cannot be waited here: {exc}")
```

# Misc
## Instance reuse after `stop()`
```python
    # You can reuse same aqute instance after proper stop() call
    aqute = Aqute(handle_coro=handler,workers_count=5)
    async with aqute:
        for i in range(10):
            await aqute.add_task(i)
        await aqute.wait_till_end()

    async with aqute:
        for i in range(10, 20):
            await aqute.add_task(i)
        await aqute.wait_till_end()

    assert aqute.result_queue.qsize() == 20
```

## Type checking and generics
You should get error during type check if you would try to use wrong type with
`Aqute` methods (types are indered based on your provided handler):
```python
from aqute import Aqute

async def handler(i: int) -> str:
    return f"success {i}"


async def main() -> None:
    aqute = Aqute(
        handle_coro=handler,
        workers_count=10
    )
    # Mypy error: error: Argument 1 to "add_task" of "Aqute" has incompatible type "str"; expected "int"  [arg-type]
    await aqute.add_task("10") 
```

You can also provide the expected types of in/out via generics mechanism:
```python
from aqute import Aqute

async def handler(i: int) -> str:
    return f"success {i}"


async def main() -> None:
    # Mypy error: Argument "handle_coro" to "Aqute" has incompatible type "Callable[[int], Coroutine[Any, Any, str]]"; expected "Callable[[int], Coroutine[Any, Any, int]]"  [arg-type]
    aqute = Aqute[int, int](
        handle_coro=handler,
        workers_count=10
    )

    await aqute.add_task(123)
```
