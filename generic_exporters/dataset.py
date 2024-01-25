
from datetime import datetime
from typing import TYPE_CHECKING, Dict, TypeVar

if TYPE_CHECKING:
    from generic_exporters.timeseries import TimeDataRow, TimeSeries, WideTimeSeries


_DT = TypeVar("_DT", "TimeSeries", "WideTimeSeries", "TimeDataRow")

class Dataset(Dict[datetime, _DT]):
    """A Dataset is a not-yet-materialized"""
    def __init__(self, data: _DT) -> None:
        self._data = data
    # TODO: implement
    def plot(self):
        raise NotImplementedError
    def to_csv(self):
        raise NotImplementedError
    def export(self, datastore):
        raise NotImplementedError
