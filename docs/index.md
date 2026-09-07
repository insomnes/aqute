# Aqute

Aqute processes coroutine calls with a worker pool, optional rate limits, and
retries. It requires Python 3.11 or newer and has no runtime dependencies.
It runs in one process; pending work and results do not survive process termination.

## Quickstart

Install the library with `pip install aqute`. This complete example returns an
ordered list of doubled values and propagates handler errors.

```python
--8<-- "examples/quickstart.py"
```

From a development checkout, run `uv run --locked python -m examples.quickstart`.

`process_all(items)` returns a list in input order. `iter_results(items)` yields
terminal results in completion order. Both accept `Iterable` and `AsyncIterable`
inputs. Each `AquteTask` exposes `data`, `task_id`, `result`, `error`, and `success`.
Inspect `error` or `success`: `None` can be a valid handler result.

See [usage](usage.md) for buffering, cleanup, retry, timeout, and compatibility
contracts. The executable examples cover [streaming](streaming.md), a shared
[HTTP client](http_client.md), and [service shutdown](service_shutdown.md).
