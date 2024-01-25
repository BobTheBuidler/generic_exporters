
from abc import abstractmethod, abstractproperty
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import AsyncGenerator, Dict, Generic, TypeVar

import a_sync

from generic_exporters.plan import QueryPlan


_T = TypeVar('_T')

class _ProcessorBase(a_sync.ASyncGenericBase, Generic[_T]):
    def __init__(self, *, sync: bool = True):
        if not isinstance(sync, bool):
            raise TypeError(f'`sync` must be boolean. You passed {sync}')
        self.sync = sync
    def __await__(self) -> _T:
        return self.run(sync=False).__await__()
    @abstractmethod
    async def run(self) -> _T:
        """Runs the processor"""


class _TimeSeriesProcessorBase(_ProcessorBase):
    interval: timedelta
    def __init__(
        self, 
        query: QueryPlan, 
        *,
        sync: bool = True,
    ) -> None:
        super().__init__(sync=sync)
        if not isinstance(query, QueryPlan):
            raise TypeError(f'`query` must be `QueryPlan`. You passed {query}')
        self.query = query
    @abstractmethod
    async def start_timestamp(self) -> datetime:
        """Returns the start of the historical range for this processor."""
    async def _timestamps(self) -> AsyncGenerator[datetime, None]:
        """Generates the timestamps to process"""
        timestamp: datetime = await self.start_timestamp(sync=False)
        timestamp = timestamp.astimezone(tz=timezone.utc)
        while timestamp < datetime.now(tz=timezone.utc) - self.interval:
            yield timestamp
            timestamp += self.interval


class _GatheringTimeSeriesProcessorBase(_TimeSeriesProcessorBase):
    """Inherit from this class when you need to collect all the data before processing"""
    async def _gather(self) -> Dict[datetime, Decimal]:
        return await a_sync.gather({ts: self.timeseries.metric.produce(ts, sync=False) async for ts in self._timestamps()})
