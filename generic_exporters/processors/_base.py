
from abc import abstractmethod
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import AsyncGenerator, Dict, Generic, TypeVar, Union

import a_sync

from generic_exporters.metric import Metric
from generic_exporters.timeseries import _TimeSeriesBase, TimeSeries


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


Processable = Union[Metric, _TimeSeriesBase]

class _TimeSeriesProcessorBase(_ProcessorBase):
    interval: timedelta
    def __init__(self, timeseries: Processable, *, sync: bool = True) -> None:
        super().__init__(sync=sync)
        if isinstance(timeseries, Metric):
            timeseries = TimeSeries(timeseries)
        if not isinstance(timeseries, _TimeSeriesBase):
            raise TypeError(f'`timeseries` must be `Metric`, `TimeSeries`, or `WideTimeSeries`. You passed {timeseries}')
        self.timeseries = timeseries
    @abstractmethod
    async def start_timestamp(self) -> datetime:
        """Returns the start of the historical range for this processor."""
    async def _timestamps(self) -> AsyncGenerator[datetime, None]:
        """Generates the timestamps to process"""
        timestamp = await self.start_timestamp(sync=False)
        while timestamp < datetime.now(tz=timezone.utc) - self.interval:
            yield timestamp
            timestamp += self.interval


class _GatheringTimeSeriesProcessorBase(_TimeSeriesProcessorBase):
    """Inherit from this class when you need to collect all the data before processing"""
    def __init__(self, timeseries: "TimeSeries", *, interval: timedelta = timedelta(days=1), buffer: timedelta = timedelta(minutes=5), sync: bool = True) -> None:
        super().__init__(timeseries, sync=sync)
        self.interval = interval
        self.buffer = buffer

    async def _gather(self) -> Dict[datetime, Decimal]:
        return await a_sync.gather({ts: self.timeseries.metric.produce(ts, sync=False) async for ts in self._timestamps()})
