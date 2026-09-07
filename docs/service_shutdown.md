# Service shutdown

Stop new input and let the current submission finish while results drain. A
deadline cancels remaining work and waits for cleanup. The returned boolean reports
whether draining timed out. Cancellation is cooperative; the deadline cannot
force a coroutine to terminate. The demonstration uses finite local work.

```bash
uv run --locked python -m examples.service_shutdown
```

```python
--8<-- "examples/service_shutdown.py"
```

See [usage](usage.md) for the complete lifecycle and error contracts.
