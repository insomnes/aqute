# Resumable batch

Aqute schedules attempts; the application owns checkpoints and idempotency.
Save each successful output with a stable completion key. On restart, read the
saved keys and submit only inputs without a completion record.

This recipe uses Python's `sqlite3` module and needs no additional dependency.
It is a single-process application with one checkpoint writer and unique inputs.
It is not a durable queue. Pending work and in-flight tasks still die with the
process; the application must retain or regenerate the original input batch.

## Run the offline demonstration

After installing `aqute==0.10.3`, save the source below as `resumable_batch.py`
and run `python resumable_batch.py`. From a development checkout, run:

```bash
uv run --locked python examples/resumable_batch.py
```

The first run submits eight inputs and stops after saving three results. The
managed stream awaits producer and worker cleanup. A second run opens the same
database, logs `Submitting 5 unfinished inputs`, and saves the five remaining
results. The final log reports `Resume: 5 new completions; 8 stored outputs`.
Assertions check the completion counts and all stored key/output pairs.

Both runs happen in one process to demonstrate stopping and resuming without
killing the script. The database connection closes between runs. The temporary
directory is removed when the demonstration finishes. For restart across process
exits, replace `TemporaryDirectory()` with a persistent application-owned path
and run the complete batch against the same database each time.

## Checkpoint boundary

`completion_key()` combines the integer input with the operation version
`double:v1`. Keep keys stable across restarts. Include every input and setting
that changes the result; for inference, include the model and request
configuration. Change the version when the operation changes. This example
assumes unique keys within the original batch.

The consumer calls `task.unwrap()` before writing. A terminal handler error
raises and leaves that input without a checkpoint; earlier records remain.
Each successful result uses one SQLite `INSERT` to store its key and output
together. `isolation_level=None` enables autocommit, so no transaction remains
open while the consumer waits for the next result. A database write failure
also stops the batch and propagates to the caller.

A crash after the handler completes but before the checkpoint commits can
repeat that input on restart. The same applies to results that finish but are
not consumed before the early stop. This is **at-least-once processing**, not
exactly-once execution. Handlers and their side effects must tolerate repetition;
use a service's idempotency key where supported. The local SQLite transaction
cannot make an external side effect atomic with its checkpoint.

SQLite calls are synchronous and can block the event loop during disk I/O.
This small recipe loads completed keys and pending inputs into memory. Those
objects, the input source, and stored outputs are outside Aqute's queue bounds.
There is no cross-process coordination or recovery of in-flight tasks here.

## Runnable source

<!-- example: examples/resumable_batch.py -->
```python
"""Resume an offline batch with application-owned SQLite completion records."""

import asyncio
import logging
import sqlite3
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory

from aqute import Aqute

logger = logging.getLogger(__name__)
FIRST_RUN_LIMIT = 3


def completion_key(value: int) -> str:
    return f"double:v1:{value}"


async def handle(value: int) -> int:
    await asyncio.sleep(0.025)  # Simulate asynchronous I/O.
    return value * 2


async def run_batch(
    inputs: range, database: Path, *, stop_after: int | None = None
) -> int:
    # Autocommit keeps each statement in its own transaction; close on every exit.
    with closing(sqlite3.connect(database, isolation_level=None)) as checkpoint:
        checkpoint.execute(
            "CREATE TABLE IF NOT EXISTS completions "
            "(key TEXT PRIMARY KEY, output INTEGER NOT NULL)"
        )
        completed_keys = {
            row[0] for row in checkpoint.execute("SELECT key FROM completions")
        }
        pending = [
            value for value in inputs if completion_key(value) not in completed_keys
        ]
        logger.info("Submitting %s unfinished inputs", len(pending))
        engine = Aqute(handle, workers_count=3)
        completed = 0
        async with engine.iter_results(pending) as results:
            async for task in results:
                output = task.unwrap()
                checkpoint.execute(
                    "INSERT INTO completions (key, output) VALUES (?, ?)",
                    (completion_key(task.data), output),
                )
                completed += 1
                logger.info("Checkpointed %s: %s", task.data, output)
                if completed == stop_after:
                    break
        return completed


async def main() -> None:
    inputs = range(8)
    with TemporaryDirectory() as directory:
        database = Path(directory) / "checkpoint.sqlite"
        first = await run_batch(inputs, database, stop_after=FIRST_RUN_LIMIT)
        assert first == FIRST_RUN_LIMIT
        logger.info("First run stopped after %s checkpoints", first)
        resumed = await run_batch(inputs, database)
        assert resumed == len(inputs) - first
        with closing(sqlite3.connect(database)) as checkpoint:
            outputs = dict(checkpoint.execute("SELECT key, output FROM completions"))
        assert outputs == {completion_key(value): value * 2 for value in inputs}
        logger.info(
            "Resume: %s new completions; %s stored outputs", resumed, len(outputs)
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
```
<!-- /example -->

## JSONL alternative

For a small single-writer batch, an append-only JSONL file can replace the table.
At startup, read each record's `key` into `completed_keys`, then use the same
pending-input filter. Treat repeated keys as one completion; the last complete
record supplies its output. For each successful result, append both fields:

```python
import json
import os

with checkpoint_path.open("a", encoding="utf-8") as checkpoint:
    checkpoint.write(json.dumps({"key": key, "output": output}) + "\n")
    checkpoint.flush()
    os.fsync(checkpoint.fileno())
```

Use a persistent path and open the file once around the result stream in an
application. JSONL does not provide SQLite's transaction boundary: interruption
can leave an incomplete final line. Before resuming, inspect and remove only
that incomplete tail, then load the complete records. Stop on other malformed
records instead of silently skipping them. Outputs must be JSON-serializable,
and the handler still needs to tolerate repetition. Prefer SQLite when manual
tail repair is unsuitable.
