# Streaming results

Use streaming to process results as they complete. Start with a fresh engine and
consume each terminal task inside the managed context. For an ordered finite batch
or continuous manual submission, see the [API selector](usage.md#for-coding-agents).

From a development checkout, run this finite example offline:

```bash
uv run --locked python -m examples.streaming
```

```python
--8<-- "examples/streaming.py"
```

The source yields integers without building an input list. Replace `source()`
with your iterable or async iterable, and `handle()` with your async operation.
`task.unwrap()` returns the successful value or raises the terminal handler error.
The loop logs each result immediately and retains no list of outputs.

The example breaks after consuming three results. Context exit stops the producer
and workers and closes the started generator source. The command logs three
results, `Source closed`, and `Results: 3 consumed`. Results arrive in completion
order; do not depend on input order or assume remaining inputs will complete.
Remove the `break` condition to consume the whole finite source.

Omitted queue limits give each input and result queue capacity three, matching
`workers_count`. Slow consumption applies backpressure. These limits bound items,
not payload bytes. Data retained by your source, logging, or application remains
outside the bound. See [buffering](usage.md#buffering) for the complete item bound
and capacity overrides.

The context also awaits cleanup when the source or consumer raises or the caller
is cancelled. Sources and handlers must cooperate with cancellation. Callers own
source resources other than started generators; see
[cleanup ownership](usage.md#streaming-and-cleanup).

Continue with [retry backoff and progress](retry_progress.md) when you need retry
policy and counter snapshots, or [bounded HTTP processing](http_client.md) for a
shared-client recipe.
