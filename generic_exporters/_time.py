
from typing import Iterable

import a_sync

from generic_exporters import _types

class _TimeDataBase(a_sync.ASyncGenericBase):
    """A base class for all timeseries data, materialized or not-yet-materialized"""
    def __init__(self, fields: Iterable[_types.SingleProcessable], *, sync: bool = True) -> None:
        self.fields = list(fields)
        self.sync=sync
