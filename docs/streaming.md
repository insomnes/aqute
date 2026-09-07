# Streaming, backoff, and progress

This finite example reads asynchronous input into bounded queues. One handler
call fails once, then succeeds after a capped retry delay with jitter. Counters
are logged during consumption. The iterator context closes workers on early exit.

```bash
uv run --locked python -m examples.streaming
```

```python
--8<-- "examples/streaming.py"
```

See [usage](usage.md) for the complete lifecycle and error contracts.
