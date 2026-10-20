# API reference

This reference generates signatures and descriptions from the Python source in
the checkout used to build the site. Published builds follow `main`.

| Page | Contents |
| --- | --- |
| [Engine](engine.md) | `Aqute` configuration, processing helpers, manual lifecycle, and counters. |
| [Tasks and counters](tasks.md) | `AquteTask` fields and `unwrap()`, plus `AquteCounters` fields. |
| [Rate limiters](rate-limiters.md) | The `RateLimiter` protocol and all five implementations. |
| [Exceptions](exceptions.md) | The three exception classes exported from `aqute`. |
| [Worker queues](worker-queues.md) | The lower-level `Foreman` lifecycle and its result limitations. |

Start with the [quickstart](../index.md) for runnable examples.
The [usage guide](../usage.md) explains buffering, retries, cancellation, and
cleanup. [Behavior and rate limits](../reference.md) covers rate-limit semantics
and synchronous I/O handlers.
