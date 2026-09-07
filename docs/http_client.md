# Shared HTTP client

Use one HTTPX client for all handler calls. Transport failures are retried; HTTP
status errors propagate to the caller. The executable demonstration uses an offline
transport and makes no network requests. HTTPX is a development dependency only.
Call `fetch_pages(urls)` without a transport for real requests.

```bash
uv run --locked python -m examples.http_client
```

```python
--8<-- "examples/http_client.py"
```

The retry callback is included in the [streaming example](streaming.md).

See [usage](usage.md) for the complete lifecycle and error contracts.
