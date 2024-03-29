
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from functools import cached_property
from inspect import isawaitable
from typing import (TYPE_CHECKING, Any, AsyncGenerator, Awaitable, Coroutine, Dict, 
                    Iterable, Iterator, Mapping, Optional, Tuple, TypeVar, Union, final)

import a_sync
from bqplot import Figure
from pandas import DataFrame

from generic_exporters import _types
from generic_exporters._awaitable import _AwaitableMixin
from generic_exporters._time import _TimeDataBase
from generic_exporters.dataset import Dataset
from generic_exporters.timeseries import _TimeSeriesBase, TimeSeries, WideTimeSeries

if TYPE_CHECKING:
    from generic_exporters import Metric
    _M = TypeVar('_M', bound=Metric)
else:
    _M = TypeVar('_M')

_T = TypeVar('_T', TimeSeries, WideTimeSeries)

LooseDatetime = Union[datetime, Awaitable[datetime]]
ReturnValue = Union[Decimal, Exception]

logger = logging.getLogger(__name__)


class _QueryPlan(_TimeDataBase, a_sync.ASyncIterable["TimeDataRow[_M]"], _AwaitableMixin[Dict[_M, _T]]):
    def __init__(
        self, 
        dataset: "Union[TimeSeries, WideTimeSeries]", 
        start_timestamp: LooseDatetime, 
        end_timestamp: datetime, 
        interval: timedelta,
        *,
        sync: bool = True,
    ) -> None:
        """Initializes a new QueryPlan instance."""
        if not isinstance(dataset, _TimeSeriesBase):
            raise TypeError(f'`dataset` must be `TimeSeries` or `WideTimeSeries`. You passed {dataset}')
        super().__init__(dataset.metrics, sync=sync)
        self.dataset = dataset
        _validate_time_params(start_timestamp, end_timestamp, interval)
        self.end_timestamp = end_timestamp
        self.interval = interval
        self._start_timestamp = start_timestamp
        self._rows: Dict[datetime, TimeDataRow] = {}
    
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} for {self.dataset} interval={self.interval}>"
    
    def __getitem__(self, timestamp: datetime) -> "TimeDataRow":
        # TODO: check that timestamp is in the data lol. maybe just populate _rows on init?
        if timestamp not in self._rows:
            self._rows[timestamp] = TimeDataRow(timestamp, self.metrics, self.sync)
        return self._rows[timestamp]
    
    async def __aiter__(self) -> AsyncGenerator[Tuple[datetime, Dict["Metric", Decimal]], None]:
        """Asynchronously iterates over the data points generated by the query plan."""
        async for timestamp in self._aiter_timestamps():
            self[timestamp]
        async for timestamp, row in a_sync.as_completed(self._rows, aiter=True):
            yield timestamp, row
    
    async def plot(self) -> Figure:
        """Generates a plot for the queried data.

        Returns:
            Figure: A plot representing the queried time series data.
        """
        from generic_exporters.processors import Plotter
        return await Plotter(self, interval=self.interval)
    
    async def dataframe(self) -> DataFrame:
        """Converts the queried data to a pandas DataFrame.

        Returns:
            DataFrame: A pandas DataFrame representing the queried time series data.
        """
        from generic_exporters.processors import DataFramer
        return await DataFramer(self, interval=self.interval)
    
    async def _materialize(self) -> Dataset:
        """Materializes the query plan, executing the necessary asynchronous operations and returning the results."""
        return Dataset(await a_sync.map(self.__getitem__, self._aiter_timestamps()))

    async def start_timestamp(self) -> datetime:
        """Computes the start timestamp for the query, handling asynchronous resolution if necessary."""
        return await self._start_timestamp if isawaitable(self._start_timestamp) else self._start_timestamp

    async def _aiter_timestamps(self, run_forever: bool = False) -> AsyncGenerator[datetime, None]:
        """Generates the timestamps to be queried based on the specified range and interval."""
        timestamp: datetime = await self.start_timestamp(sync=False)
        timestamp = timestamp.astimezone(tz=timezone.utc)
        if run_forever is True:
            while True:
                while not _ts_is_ready(timestamp, self.interval):
                    await asyncio.sleep((datetime.now(tz=timezone.utc) - self.interval - timestamp).total_seconds())
                yield timestamp
                timestamp += self.interval
        elif run_forever is False:
            while _ts_is_ready(timestamp, self.interval):
                yield timestamp
                timestamp += self.interval
        else:
            raise TypeError(f'`run_forever` must be boolean. You passed {run_forever}')


@final
class QueryPlan(_QueryPlan["Metric", Decimal]):
    """Represents a plan for querying time series data within a specified range.

    This class encapsulates the logic for generating and executing a plan to query
    time series data. It supports asynchronous iteration over the generated data points
    and provides methods for plotting and converting the data to a pandas DataFrame.

    Attributes:
        dataset (Union[TimeSeries, WideTimeSeries]): The dataset to be queried.
        start_timestamp (LooseDatetime): The start of the query range.
        end_timestamp (datetime): The end of the query range.
        interval (timedelta): The interval between data points in the query.
        sync (bool): A flag indicating whether operations should be performed synchronously or asynchronously.

    Args:
        dataset (Union[TimeSeries, WideTimeSeries]): The dataset to be queried.
        start_timestamp (LooseDatetime): The start of the query range.
        end_timestamp (datetime): The end of the query range.
        interval (timedelta): The interval between data points in the query.
        sync (bool, optional): Specifies if operations should be executed synchronously. Defaults to True.
    """


def _ts_is_ready(timestamp: datetime, interval: timedelta) -> bool:
    return timestamp < datetime.now(tz=timezone.utc) - interval


def _validate_time_params(start_timestamp: datetime, end_timestamp: Optional[datetime], interval: timedelta):
    """Validates the time parameters for the query plan."""
    if not isinstance(start_timestamp, datetime) and not isawaitable(start_timestamp):
        raise TypeError(f"`start_timestamp` must be `datetime` or `Awaitable[datetime]`. You passed {start_timestamp}")
    if end_timestamp and not isinstance(end_timestamp, datetime):
        raise TypeError(f"`end_timestamp` must be `datetime`. You passed {end_timestamp}")
    if not isinstance(interval, timedelta):
        raise TypeError(f"`interval` must be `timedelta`. You passed {interval}")        


class _TimeDataRow(_TimeDataBase, _AwaitableMixin[Dict[_M, Decimal]], Mapping[str, Coroutine[Any, Any, Decimal]]):
    # TODO: support time-based (and non time based tbh) attrs like we do metrics
    # TODO: give this more functionality waaaay later
    """A future-like class that can be awaited to materialize results. You can subclass this for type checking with your own `Metric` subtypes."""
    __slots__ = 'timestamp', 
    def __init__(self, timestamp: datetime, fields: Iterable[_types.SingleProcessable], sync: bool = True) -> None:
        super().__init__(fields, sync=sync)
        self.timestamp = timestamp
    def __repr__(self) -> str:
        beg = f"<{type(self).__name__}(timestamp={self.timestamp}"
        mid = "".join(f",field{i}={self.metrics[i]}" for i in range(len(self.metrics)))
        end = ")>"
        return beg + mid + end
    def __getitem__(self, key: str) -> Any:
        for metric in self.metrics:
            if key == metric.key:
                return metric.produce(self.timestamp, sync=self.sync)
        raise KeyError(key)
    def __iter__(self) -> Iterator[str]:
        for metric in self.metrics:
            yield metric.key
    def __len__(self) -> int:
        return len(self.metrics)
    @property
    def key(self) -> datetime:
        return self.timestamp
    @cached_property
    def tasks(self) -> a_sync.TaskMapping[_M, Decimal]:
        return a_sync.map(lambda metric: metric.produce(self.timestamp, sync=False), self.metrics)
    async def _materialize(self) -> Dict[_M, ReturnValue]:
        return await self.tasks

@final
class TimeDataRow(_TimeDataRow["Metric"]):
    """A future-like class that can be awaited to materialize results"""
