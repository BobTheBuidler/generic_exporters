
from abc import ABCMeta, abstractmethod, abstractproperty
from decimal import Decimal
from typing import Optional, TypeVar

from generic_exporters.datastores.timeseries._base import TimeSeriesDataStoreBase
from generic_exporters.datastores.timeseries.sql import SQLTimeSeriesKeyValueStore


_T = TypeVar('_T')


class _ExporterABC(metaclass=ABCMeta):
    @abstractmethod
    async def data_exists(self) -> bool:
        """Returns True if data exists, False if it does not and must be produced."""


class _MetricExporterABC(_ExporterABC):
    def __init__(self, datastore: Optional[TimeSeriesDataStoreBase]) -> None:        
        if isinstance(datastore, TimeSeriesDataStoreBase):
            self.datastore = datastore
        elif datastore is None:
            self.datastore = SQLTimeSeriesKeyValueStore()
        else:
            raise TypeError(datastore)
        self.datastore = datastore
    def __await__(self):
        return self.export().__await__()
    @abstractproperty
    def metric_name(self) -> str:
        """A human-readable label for the metric being exported"""
    @abstractmethod
    async def produce(self) -> Decimal:
        """Produces data for exporting."""
    @abstractmethod
    async def export(self) -> None:
        """Exports the full history of the data to the data store"""


class _PropertyExporterABC(_ExporterABC):
    output_type: _T
    @abstractproperty
    def property_name(self) -> str:
        pass
    @abstractmethod
    async def produce(self) -> _T:
        pass
