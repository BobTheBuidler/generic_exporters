
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Iterable, List, Optional, Union, final

from generic_exporters._time import _TimeDataBase

if TYPE_CHECKING:
    from generic_exporters import Metric
    from generic_exporters.plan import QueryPlan


class _TimeSeriesBase(_TimeDataBase):
    def __getitem__(self, key: "slice[datetime, Optional[datetime], timedelta]") -> "QueryPlan":
        """Slice time and return a `Query` object representing the not-yet-fetched series of Metric values for all timestamps in the slice"""
        if not isinstance(key, slice):
            raise KeyError(f"key should be a slice object with a datetime start value, an Optional[datetime] stop, and an Optional[timedelta] step. You passed {key}")
        if not isinstance(key.start, datetime):
            raise TypeError(f"The start index must be a datetime. You passed {key.start}.")
        if key.start > datetime.utcnow():
            ValueError(f"The start index must be < the current time, {datetime.utcnow()}. You passed {key.stop}")
        if key.stop:
            if not isinstance(key.stop, datetime):
                raise TypeError(f"The stop index must be a datetime. You passed {key.stop}.")
            if key.stop > datetime.utcnow():
                raise ValueError(f"The stop index must be <= the current time, {datetime.utcnow()}. You passed {key.stop}")
        if key.step and not isinstance(key.step, timedelta):
            raise TypeError(f"The slice step must be a timedelta. You passed {key.step}.")
        # prevent circular import
        from generic_exporters.plan import QueryPlan
        return QueryPlan(self, key.start, key.stop, key.step or timedelta(seconds=((key.stop or datetime.utcnow()) - key.start).total_seconds() / 1_000))


@final
class TimeSeries(_TimeSeriesBase):
    """
    An object representing the infinite series of values for a particular `Metric` across the time axis. 
    NOTE: Imagine a line chart with a single line that has yet to be drawn.

    You can slice a `TimeSeries` object to create a `Dataset` which can be used for exporting, plotting, and other fun things #NOTE: not yet implemented

    tcollection of asyncio.Tasks that each will return one datapoint for a specific timestamp for a `Metric` object."""
    def __init__(self, metric: "Metric", *, sync: bool = True) -> None:
        """
        metric: the Metric that the TimeSeries will measure
        """
        # prevent circular import
        from generic_exporters import Metric
        if not isinstance(metric, Metric):
            raise TypeError(f'`metric` must be a `Metric` object. You passed {metric}')
        self.metric = metric
        super().__init__([metric], sync=sync)
    @property
    def key(self) -> str:
        return self.metric.key
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} for metric={self.metric} start={self.start_timestamp} end={self.end_timestamp} interval={self.interval}>"
    def __add__(self, other: Union["TimeSeries", "Metric"]) -> "TimeSeries":
        self.__validate_other(other)
        return TimeSeries(self.metric + other.metric)
    def __sub__(self, other: Union["TimeSeries", "Metric"]) -> "TimeSeries":
        self.__validate_other(other)
        return TimeSeries(self.metric - other.metric)
    def __mul__(self, other: Union["TimeSeries", "Metric"]) -> "TimeSeries":
        self.__validate_other(other)
        return TimeSeries(self.metric * other.metric)
    def __truediv__(self, other: Union["TimeSeries", "Metric"]) -> "TimeSeries":
        self.__validate_other(other)
        return TimeSeries(self.metric / other.metric)
    def __floordiv__(self, other: Union["TimeSeries", "Metric"]) -> "TimeSeries":
        self.__validate_other(other)
        return TimeSeries(self.metric // other.metric)
    def __power__(self, other: Union["TimeSeries", "Metric"]) -> "TimeSeries":
        self.__validate_other(other)
        return TimeSeries(self.metric ** other.metric)
    def __validate_other(self, other: Union["TimeSeries", "Metric"]) -> None:
        from generic_exporters import Metric
        if not isinstance(other, (TimeSeries, Metric)):
            raise TypeError(f"`other` must be `TimeSeries` or `Metric`. You passed {other}.")


@final
class WideTimeSeries(_TimeSeriesBase):
    """
    A collection of `TimeSeries` objects
    NOTE: Imagine a line chart with multiple lines that have yet to be drawn
    """
    def __init__(self, *timeserieses: Union[TimeSeries, "Metric"], sync: bool = True) -> None:
        if not timeserieses or len(timeserieses) == 1:
            raise ValueError("You must provide 2 or more `TimeSeries` or `Metric` objects")
        timeserieses = _convert_metrics(timeserieses)
        for i in range(len(timeserieses)-2):
            timeserieses[i].__validate(timeserieses[i+1])
        self.fields = timeserieses
        self.sync = sync
        self._rows = {}
    @property
    def key(self) -> str:
        raise NotImplementedError("Preventing this object from being used incorrectly, will refactor out eventually maybe")

def _convert_metrics(items: Iterable[Union[TimeSeries, "Metric"]]) -> List[TimeSeries]:
    items = list(items)
    for i in range(len(items)):
        if not isinstance(items[i], TimeSeries):
            items[i] = TimeSeries(items[i])
    return items

