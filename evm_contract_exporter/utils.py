
import logging
from datetime import datetime

from a_sync.primitives.locks.prio_semaphore import PrioritySemaphore
from async_lru import alru_cache
from y.time import closest_block_after_timestamp_async

logger = logging.getLogger(__name__)

block_timestamp_semaphore = PrioritySemaphore(500, "block for timestamp semaphore")

@alru_cache(maxsize=None)
async def get_block_by_timestamp(timestamp: datetime) -> int:
    async with block_timestamp_semaphore[0 - timestamp.timestamp()]:  # NOTE: We invert the priority to go high-to-low
        logger.debug("getting block at %s", timestamp)
        block = await closest_block_after_timestamp_async(timestamp) - 1
        logger.debug("block at %s is %s", timestamp, block)
        return block
