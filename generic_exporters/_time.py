
from typing import TYPE_CHECKING, Iterable, Union

import a_sync

if TYPE_CHECKING:
    from generic_exporters import Metric, TimeSeries

class _TimeDataBase(a_sync.ASyncGenericBase):
    """A base class for all timeseries data, materialized or not-yet-materialized"""
    def __init__(self, fields: Iterable[Union["Metric", "TimeSeries"]], *, sync: bool = True) -> None:
        self.fields = list(fields)
        self.sync=sync
