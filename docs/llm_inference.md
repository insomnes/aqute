# LLM batch inference

Use Aqute for independent remote inference requests when the application needs
bounded concurrency, selected retries, and incremental results. This example adds
a token-cost budget to the [HTTP recipe](http_client.md). It uses HTTPX directly;
it does not require a vendor SDK, credentials, or a running model server.

From a development checkout, run it offline:

```bash
uv run --locked python -m examples.llm_inference
```

The mock endpoint returns chat-completions-shaped JSON with a `usage` field.
It delays responses by 25–100 ms, returns one HTTP 429 with `Retry-After: 1`,
then completes three classifications. A second run skips the checkpointed inputs
and makes no requests. The model name, endpoint, input-token estimates, and usage
values are placeholders for this demonstration.

## Runnable source

```python
--8<-- "examples/llm_inference.py"
```

The checkout provides `httpx` in its development dependencies. To adapt the source
outside the checkout, install `aqute==0.10.0` and `httpx`, and replace the endpoint,
model, credentials, and token estimates with your application's configuration.
The source imports `RateLimitedError` and `retry_after_seconds` from
`examples.http_client`; copy those definitions from the [HTTP source](http_client.md#runnable-source)
or supply equivalents. The `examples` package is not installed with Aqute.

## Requests and token budgets

RPM counts requests per minute; TPM counts tokens per minute. Limiting request
starts alone does not limit tokens when prompts and output lengths differ.
`workers_count=3` limits occupied workers independently of either quota.
This example controls estimated TPM; it does not add a separate RPM limiter.

`TokenCostLimiter` implements the public `RateLimiter.acquire(name, task)`
protocol in application code. Its cost callback reads `task.data` and reserves
`estimated_input_tokens + max_tokens` before every attempt, including retries.
The bucket starts full, refills continuously at 600 tokens per minute, and holds
at most 600 tokens. One request's positive estimated cost must fit that capacity;
an invalid cost raises `ValueError` and stops the run. Initial bursts are possible:
this token bucket does not enforce a strict rolling-minute ceiling.

This is an **upper-bound estimate without refund**, conditional on the input
estimate covering the actual input and `max_tokens` bounding the counted output.
The supplied estimates are illustrative, not a tokenizer. Provider accounting
differs by vendor and can include other token categories or reservation rules;
the local limiter is a budget model, not a mirror of provider accounting.
The response's `usage` is logged only. Failed attempts keep their reservation,
and a successful response does not return unused tokens to the bucket.

This conservative reservation underutilizes the budget when `max_tokens` greatly
exceeds typical output length. Set `max_tokens` to the task's required output
size, not an arbitrary safety margin. A refund based on reported usage could be
an application extension for free-form generation, but is not implemented here.

## Retries and throttling

`PausableRateLimiter` wraps the token limiter. On HTTP 429, the handler parses
`Retry-After`, extends the shared pause, and raises a retryable error.
Already admitted requests can still complete or return 429. Admissions delayed
by the pause can resume together; this wrapper does not guarantee spacing after
the pause. Header parsing and fallback behavior follow the [HTTP recipe](http_client.md#limits-and-outcomes).

`retry_count=2` allows at most three attempts per input. Only HTTP 429 and
`httpx.TransportError` are retried, with a 0.1-second per-worker delay in addition
to any remaining admission wait. Other HTTP errors propagate through
`task.unwrap()`, stop the batch, and leave earlier checkpoints intact.
The five-second HTTPX timeout applies to network operations, not the whole batch,
token-budget waits, or shared pauses.

There is no SDK retry layer in this example; the real HTTP transport explicitly
uses `retries=0`, and the mock transport performs no automatic retries.
If you replace HTTPX with an SDK, disable its internal retries (often
`max_retries=0`) and let Aqute retry. Otherwise one handler attempt can make
multiple requests after a single token reservation. Disable retries on one side
to avoid multiplication: `retry_count=0` avoids Aqute retries, but SDK-internal
attempts would still bypass this limiter. Disable SDK retries when the limiter
must see every attempt. Select retries with your provider's billing and duplicate
request behavior in mind; a transport failure does not prove inference never ran.

## Checkpoints and cleanup

After consuming a successful result, the application stores
`checkpoint[task.data] = output`. A new batch filters those inputs out before
submission. This demonstration keeps the checkpoint in memory, keyed by the full
`Prompt` value. It is lost on process exit and assumes unique inputs for a fixed
model and configuration. Duplicate inputs within one run can still be submitted.

For resumable application work, persist the output and completion key together
and include the model and relevant request configuration in that key. A crash
after inference but before persistence can repeat a request; this example does
not provide exactly-once execution or a durable queue. Application checkpoints
also retain data outside Aqute's queue bounds. Each batch creates a fresh limiter,
so restarting a batch resets the local budget and shared pause.

The managed result stream waits for producer and worker cleanup before the HTTP
client closes. This requires cooperative cancellation. The example buffers each
JSON response in memory; finite queues bound item counts, not response bytes.
