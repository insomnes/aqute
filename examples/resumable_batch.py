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
