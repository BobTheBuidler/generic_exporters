from typing import TYPE_CHECKING, Iterable, List

import a_sync

from generic_exporters import _types

if TYPE_CHECKING:
    from generic_exporters import Metric

class _TimeDataBase(a_sync.ASyncGenericBase):
    """A base class for representing time series data, both materialized and not-yet-materialized.

    This class serves as a foundational component for time series data handling within the library.
    It is designed to be subclassed by more specific time series data structures that require
    a common interface for handling fields related to time series data.

    Attributes:
        fields (list): A list of fields that represent the data points or metrics within the time series.
        sync (bool): A flag indicating whether operations should be performed synchronously or asynchronously.

    Args:
        fields (Iterable[_types.SingleProcessable]): An iterable of fields, typically metrics or other
                                                     processable units, that make up the time series data.
        sync (bool, optional): Specifies if operations should be executed synchronously. Defaults to True.
    """
    metrics: List["Metric"]
    __slots__ = 'fields', 'sync'
    def __init__(self, fields: Iterable[_types.SingleProcessable], *, sync: bool = True) -> None:
        """Initializes a new instance of the _TimeDataBase class.

        Args:
            fields (Iterable[_types.SingleProcessable]): An iterable of fields that represent the data points
                                                         or metrics within the time series.
            sync (bool, optional): Specifies if operations should be executed synchronously. Defaults to True.
        """
        self.metrics = list(fields)
        self.sync = sync