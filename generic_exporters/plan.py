
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from functools import cached_property
from inspect import isawaitable
from typing import TYPE_CHECKING, Any, AsyncGenerator, Awaitable, Coroutine, Dict, Iterable, List, Optional, TypeVar, Union, final

import a_sync
from bqplot import Figure
from pandas import DataFrame

from generic_exporters._awaitable import _AwaitableMixin
from generic_exporters._time import _TimeDataBase
from generic_exporters.dataset import Dataset
from generic_exporters.timeseries import _TimeSeriesBase, TimeSeries, WideTimeSeries

if TYPE_CHECKING:
    from generic_exporters import Metric

_T = TypeVar('_T', TimeSeries, WideTimeSeries)

LooseDatetime = Union[datetime, Awaitable[datetime]]

logger = logging.getLogger(__name__)

@final
class QueryPlan(_TimeDataBase, a_sync.ASyncIterable["TimeDataRow"], _AwaitableMixin[_T]):
    def __init__(
        self, 
        dataset: "Union[TimeSeries, WideTimeSeries]", 
        start_timestamp: LooseDatetime, 
        end_timestamp: datetime, 
        interval: timedelta,
        *,
        sync: bool = True,
    ) -> None:
        if not isinstance(dataset, _TimeSeriesBase):
            raise TypeError(f'`dataset` must be `TimeSeries` or `WideTimeSeries`. You passed {dataset}')
        super().__init__(dataset.fields, sync=sync)
        self.dataset = dataset
        _validate_time_params(start_timestamp, end_timestamp, interval)
        self.end_timestamp = end_timestamp
        self.interval = interval
        self._start_timestamp = start_timestamp
        self._rows: Dict[datetime, TimeDataRow] = {}
    
    def __getitem__(self, timestamp: datetime) -> "TimeDataRow":
        # TODO: check that timestamp is in the data lol. maybe just populate _rows on init?
        if timestamp not in self._rows:
            self._rows[timestamp] = TimeDataRow(timestamp, self.fields, self.sync)
        return self._rows[timestamp]
    
    async def __aiter__(self) -> AsyncGenerator["Dataset[TimeDataRow]", None]:
        # NOTE: THESE DO NOT YIELD IN ORDER. QUESTION: SHOULD THEY YIELD IN ORDER? MAYBE YEAH
        async for timestamp in self._aiter_timestamps():
            self[timestamp]
        async for row in a_sync.as_completed(self._rows, aiter=True):
            yield row
    
    @cached_property
    def keys(self) -> List[str]:
        return [field.key for field in self.fields]
    
    @cached_property
    def _tasks(self) -> List[asyncio.Task]:
        return {field.key: asyncio.create_task(coro) for coro, field in zip(self._coros, self.dataset.fields)}
    
    async def plot(self) -> Figure:
        from generic_exporters.processors import Plotter  # prevent circular import
        return await Plotter(self.dataset, interval=self.interval)
    
    async def dataframe(self) -> DataFrame:
        from generic_exporters.processors import DataFramer  # prevent circular import
        return await DataFramer(self.dataset, interval=self.interval)
    
    async def to_csv(self, *args):
        # TODO: this should probably go to the Exporter class
        df = await self.dataframe()
        df.to_csv(*args)

    async def _materialize(self) -> None:
        await asyncio.gather(*self._tasks)
        return Dataset(self)

    async def start_timestamp(self) -> datetime:
        """
        Returns the start of the historical range for this processor.
        Override this if the start_timestamp needs to be dynamically computed.
        """
        return await self._start_timestamp if isawaitable(self._start_timestamp) else self._start_timestamp

    async def _aiter_timestamps(self) -> AsyncGenerator[datetime, None]:
        """Generates the timestamps to process"""
        timestamp: datetime = await self.start_timestamp(sync=False)
        timestamp = timestamp.astimezone(tz=timezone.utc)
        while timestamp < datetime.now(tz=timezone.utc) - self.interval:
            yield timestamp
            timestamp += self.interval

def _validate_time_params(start_timestamp: datetime, end_timestamp: Optional[datetime], interval: timedelta):
    if not isinstance(start_timestamp, datetime) and not isawaitable(start_timestamp):
        raise TypeError(f"`start_timestamp` must be `datetime` or `Awaitable[datetime]`. You passed {start_timestamp}")
    if end_timestamp and not isinstance(end_timestamp, datetime):
        raise TypeError(f"`end_timestamp` must be `datetime`. You passed {end_timestamp}")
    if not isinstance(interval, timedelta):
        raise TypeError(f"`interval` must be `timedelta`. You passed {interval}")


@final
class TimeDataRow(_TimeDataBase, _AwaitableMixin[_T]):
    # TODO: support time-based (and non time based tbh) attrs like metrics
    # TODO: give this more functionality waaaay later
    """A future-like class that can be awaited to materialize results"""
    __slots__ = 'timestamp', 
    def __init__(self, timestamp: datetime, fields: Iterable["Metric"], sync: bool = True) -> None:
        super().__init__(fields, sync=sync)
        self.timestamp = timestamp
    def __repr__(self) -> str:
        beg = f"<{type(self).__name__}(timestamp={self.timestamp}"
        mid = "".join(f",field{i}={self.fields[i]}" for i in range(len(self.fields)))
        end = ")>"
        return beg + mid + end
    def __getitem__(self, key: str) -> Any:
        for metric in self.fields:
            if key == metric.key:
                return metric.produce(self.timestamp, sync=self.sync)
        raise KeyError(key)
    @property
    def key(self) -> datetime:
        return self.timestamp
    @property
    def _coros(self) -> List[Coroutine[Decimal, None, None]]:
        try:
            return [metric.produce(self.timestamp, sync=False) for metric in self.fields]
        except AttributeError as e:
            raise AttributeError(str(e), self.fields)
    @cached_property
    def _tasks(self) -> List["asyncio.Task[Decimal]"]:
        return [asyncio.create_task(coro, name=f"{field.key} at {self.timestamp}") for coro, field in zip(self._coros, self.fields)]
    async def _materialize(self) -> Dict["Metric", Decimal]:
        return {field: result for field, result in zip(self.fields, await asyncio.gather(*self._tasks))}
