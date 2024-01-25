
import asyncio
from abc import abstractproperty
from datetime import datetime, timedelta
from decimal import Decimal
from functools import cached_property
from typing import TYPE_CHECKING, Any, AsyncGenerator, Awaitable, Coroutine, Dict, Iterable, List, Optional, Tuple, TypeVar, Union, final

import a_sync
from typing_extensions import Self

if TYPE_CHECKING:
    from generic_exporters import Metric
    from generic_exporters.dataset import Dataset

    
_T = TypeVar('_T')


class _TimeDataBase(a_sync.ASyncGenericBase):
    """Todo: maybe refactor into _EphemeralWindowBase or somethign like that"""
    def __init__(self, fields: Iterable[Union["Metric", "TimeSeries"]]) -> None:
        self.fields = list(fields)
    def __await__(self) -> Self:
        # NOTE: maybe put this in _Awaitable helper
        return self._task.__await__()
    async def to_csv(self):
        # TODO: this should probably go to the Exporter class
        raise NotImplementedError
    @cached_property
    def _task(self) -> "asyncio.Task[Dataset[Self]]":
        """The task that is run to materialize the `Dataset`"""
        return asyncio.create_task(self._materialize())  # TODO: name the task with some hueristic
    @cached_property
    def _tasks(self) -> List[asyncio.Task]:
        return {field.key: asyncio.create_task(coro) for coro, field in zip(self._coros, self.fields)}
    @abstractproperty
    def _coros(self) -> List[Awaitable]:
        ...
    async def _materialize(self) -> None:
        await asyncio.gather(*self._tasks)
        return Dataset(self)
    

class _TimeSeriesBase(_TimeDataBase, a_sync.ASyncIterable[_T]):
    async def plot(self):
        # TODO: this should probably go to the Exporter class or a Plotter subclass
        raise NotImplementedError


@final
class TimeSeries(_TimeSeriesBase[Decimal]):
    # TODO: subclass table (timeseries is a 2 column table: key, data)
    """
    An object representing the infinite series of values for a particular `Metric` across the time axis. 
    NOTE: Imagine a line chart with a single line that has yet to be drawn.

    You can slice a `TimeSeries` object to create a `Dataset` which can be used for exporting, plotting, and other fun things #NOTE: not yet implemented

    tcollection of asyncio.Tasks that each will return one datapoint for a specific timestamp for a `Metric` object."""
    def __init__(self, metric: "Metric", start_timestamp: datetime, end_timestamp: Optional[datetime], interval: timedelta) -> None:
        """
        metric: the Metric that the TimeSeries will measure
        # TODO: move timestamp stuff to Exporter class
        """
        self.metric = metric
        super().__init__([metric])
        # TODO: move timestamp stuff to Processor base class or maybe Window class
        if not isinstance(start_timestamp, datetime):
            raise TypeError(f"`start_timestamp` must be `datetime`. You passed {start_timestamp}")
        if end_timestamp and not isinstance(end_timestamp, datetime):
            raise TypeError(f"`end_timestamp` must be `datetime`. You passed {end_timestamp}")
        if not isinstance(interval, timedelta):
            raise TypeError(f"`interval` must be `timedelta`. You passed {interval}")
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        self.interval = interval
    @property
    def key(self) -> str:
        return self.metric.key
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} for metric={self.metric} start={self.start_timestamp} end={self.end_timestamp} interval={self.interval}>"
    def __await__(self) -> Dict[datetime, Decimal]:
        return self.get_data().__await__()
    async def __aiter__(self) -> AsyncGenerator[Tuple[datetime, Decimal], None]:
        for timestamp, task in self._tasks.items():
            yield timestamp, await task
    async def get_data(self) -> Dict[datetime, Decimal]:
        # TODO; maybe move to exporter class (or base class) or delett
        return dict(zip(self._timestamps, await asyncio.gather(*self._tasks.values())))
    
    # TODO: refactor math to base class _Mathable
    def __add__(self, other: "TimeSeries") -> "TimeSeries":
        self.__validate_math(other)
        return TimeSeries(self.metric + other.metric, self.start_timestamp, self.end_timestamp, self.interval)
    def __sub__(self, other: "TimeSeries") -> "TimeSeries":
        self.__validate_math(other)
        return TimeSeries(self.metric - other.metric, self.start_timestamp, self.end_timestamp, self.interval)
    def __mul__(self, other: "TimeSeries") -> "TimeSeries":
        self.__validate_math(other)
        return TimeSeries(self.metric * other.metric, self.start_timestamp, self.end_timestamp, self.interval)
    def __truediv__(self, other: "TimeSeries") -> "TimeSeries":
        self.__validate_math(other)
        return TimeSeries(self.metric / other.metric, self.start_timestamp, self.end_timestamp, self.interval)
    def __floordiv__(self, other: "TimeSeries") -> "TimeSeries":
        self.__validate_math(other)
        return TimeSeries(self.metric // other.metric, self.start_timestamp, self.end_timestamp, self.interval)
    def __power__(self, other: "TimeSeries") -> "TimeSeries":
        self.__validate_math(other)
        return TimeSeries(self.metric * other.metric, self.start_timestamp, self.end_timestamp, self.interval)

    def _coros(self) -> List[Awaitable[Decimal]]:
        return [self.metric.produce(ts) for ts in self._timestamps]
    @cached_property
    def _tasks(self) -> Dict[datetime, "asyncio.Task[Decimal]"]:
        self._coros
        return {timestamp: asyncio.create_task(coro, name=f"{self.metric.key} at {timestamp}") for timestamp, coro in zip(self._timestamps, self._coros)}
    @cached_property
    # TODO: move to processor base class
    def _timestamps(self) -> List[datetime]:
        return self._get_timestamps()
    async def _get_timestamps(self) -> List[datetime]:
        # TODO: move to processor base class
        return [x for x in meke_timestamps(self.start_timestamp, self.end_timestamp or datetime.utcnow())]
    async def _materialize(self) -> "Dataset[Self]":
        await self.get_data()
        return Dataset(self)
    def __validate_math(self, other: "TimeSeries") -> None:
        if not isinstance(other, TimeSeries):
            raise TypeError(f"`other` must be `TimeSeries`. You passed {other}.")
        if not all(
            self.start_timestamp == other.start_timestamp, 
            self.end_timestamp == other.end_timestamp, 
            self.interval == other.interval
        ):
            raise ValueError(f"{self} and {other} must share the same time parameters")


def _convert_metrics(items: List[Union[TimeSeries, "Metric"]]) -> List[TimeSeries]:
    for i in range(len(items)):
        if not isinstance(items[i], TimeSeries):
            items[i] = TimeSeries(items[i])
    return items

@final
class WideTimeSeries(_TimeSeriesBase["TimeDataRow"]):
    """
    A collection of `TimeSeries` objects
    NOTE: Imagine a line chart with multiple lines that have yet to be drawn
    """
    def __init__(self, *timeserieses: Union[TimeSeries, "Metric"], sync: bool = True) -> None:
        if not timeserieses or len(timeserieses) == 1:
            raise ValueError("You must provide 2 or more `TimeSeries` or `Metric` objects")
        timeserieses = _convert_metrics(timeserieses)
        for i in range(len(timeserieses)-2):
            timeserieses[i].__validate_math(timeserieses[i+1])
        self.fields = timeserieses
        self.sync = sync
        self._rows = {}
    @property
    def key(self) -> str:
        raise NotImplementedError("Preventing this object from being used incorrectly, will refactor out eventually maybe")
    @property
    def timestamps(self):
        return self.fields[0]._timestamps
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
