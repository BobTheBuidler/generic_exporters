
import asyncio
from datetime import datetime, timedelta
from decimal import Decimal
from functools import cached_property
from typing import TYPE_CHECKING, Any, AsyncGenerator, Coroutine, Iterable, Generic, List, TypeVar, Union, final

import a_sync
from bqplot import Figure
from pandas import DataFrame

from generic_exporters._time import _TimeDataBase
from generic_exporters.dataset import Dataset
from generic_exporters.timeseries import _TimeSeriesBase, TimeSeries, WideTimeSeries

if TYPE_CHECKING:
    from generic_exporters import Metric

_T = TypeVar('_T', TimeSeries, WideTimeSeries)

@final
class QueryPlan(_TimeDataBase, a_sync.ASyncIterable["TimeDataRow"], Generic[_T]):
    def __init__(
        self, 
        dataset: "Union[TimeSeries, WideTimeSeries]", 
        start_timestamp: datetime, 
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
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        self.interval = interval
        
    def __await__(self) -> _T:
        # NOTE: maybe put this in _Awaitable helper
        return self._task.__await__()
    
    def __getitem__(self, timestamp: datetime) -> "TimeDataRow":
        # TODO: check that timestamp is in the data lol. maybe just populate _rows on init?
        if timestamp not in self._rows:
            self._rows[timestamp] = TimeDataRow(timestamp, self.fields, self.sync)
        return self._rows[timestamp]
    
    async def __aiter__(self) -> AsyncGenerator["Dataset[TimeDataRow]", None]:
        # NOTE: THESE DO NOT YIELD IN ORDER. QUESTION: SHOULD THEY YIELD IN ORDER? MAYBE YEAH
        for timestamp in self.timestamps:
            if timestamp not in self._rows:
                self._rows[timestamp] = TimeDataRow(timestamp, self.fields, sync=self.sync)
        async for row in a_sync.as_completed(self._rows, aiter=True):
            yield row
    
    @cached_property
    def _task(self) -> "asyncio.Task[Dataset[_T]]":
        """The task that is run to materialize the `Dataset`"""
        return asyncio.create_task(self._materialize())  # TODO: name the task with some hueristic
    
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


def _validate_time_params(start_timestamp: datetime, end_timestamp: datetime, interval: timedelta):
    if not isinstance(start_timestamp, datetime):
        raise TypeError(f"`start_timestamp` must be `datetime`. You passed {start_timestamp}")
    if end_timestamp and not isinstance(end_timestamp, datetime):
        raise TypeError(f"`end_timestamp` must be `datetime`. You passed {end_timestamp}")
    if not isinstance(interval, timedelta):
        raise TypeError(f"`interval` must be `timedelta`. You passed {interval}")


@final
class TimeDataRow(_TimeDataBase):
    # TODO: support time-based (and non time based tbh) attrs like metrics
    # TODO: give this more functionality waaaay later
    """A future-like class that can be awaited to materialize results"""
    __slots__ = 'timestamp', 'fields', 'sync'
    def __init__(self, timestamp: datetime, fields: Iterable["Metric"], sync: bool = True) -> None:
        self.timestamp = timestamp
        self.fields = tuple(fields)
        self.sync = sync
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
        return [metric.produce(self.timestamp, sync=False) for metric in self.fields]
    @cached_property
    def _tasks(self) -> List["asyncio.Task[Decimal]"]:
        return [asyncio.create_task(coro, name=f"{field.key} at {self.timestamp}") for coro, field in zip(self._coros, self.fields)]
