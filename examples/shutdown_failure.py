"""Log and propagate a handler cleanup failure after a drain deadline."""

import asyncio
import logging

from aqute import Aqute

logger = logging.getLogger(__name__)


async def main() -> None:
    started = asyncio.Event()

    async def handle(job_id: int) -> int:
        try:
            started.set()
            await asyncio.Event().wait()
            return job_id
        finally:
            # Simulate a failure while closing the handler's external resource.
            raise OSError("remote resource cleanup failed")

    engine = Aqute(handle, 1)
    try:
        async with engine:
            await engine.add_task(1)
            await started.wait()
            # An immediate drain deadline cancels the already-running handler.
            async with asyncio.timeout(0):
                await engine.finish()
    except* OSError:
        logger.exception("Handler cleanup failed during shutdown")
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
