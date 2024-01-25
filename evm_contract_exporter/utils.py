
from datetime import datetime

import a_sync
import y


block_timestamp_semaphore = a_sync.PrioritySemaphore(500, name="block for timestamp semaphore")

async def get_block_at_timestamp(timestamp: datetime) -> int:
    """Returns the number of the last block minted before the exact moment of `timestamp`"""
    async with block_timestamp_semaphore[0 - timestamp.timestamp()]:  # NOTE: We invert the priority to go high-to-low so we can get more recent data more quickly
        return await y.get_block_at_timestamp(timestamp)
