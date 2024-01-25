
from abc import abstractmethod
from datetime import datetime, timedelta, timezone
from typing import AsyncGenerator, Generic, TypeVar

import a_sync


_T = TypeVar('_T')

class _ProcessorBase(a_sync.ASyncGenericBase, Generic[_T]):
    def __init__(self, sync: bool = True):
        self.sync = sync
    def __await__(self) -> _T:
        return self.run(sync=False).__await__()
    @abstractmethod
    async def run(self) -> _T:
        """Runs the processor"""

class _TimeSeriesProcessorBase(_ProcessorBase):
    interval: timedelta
    @abstractmethod
    async def start_timestamp(self) -> datetime:
        """Returns the start of the historical range for this processor."""
    async def _timestamps(self) -> AsyncGenerator[datetime, None]:
        """Generates the timestamps to process"""
        timestamp = await self.start_timestamp(sync=False)
        while timestamp < datetime.now(tz=timezone.utc) - self.interval:
            yield timestamp
            timestamp += self.interval
