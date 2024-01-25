
from abc import abstractmethod, abstractproperty
from datetime import datetime
from decimal import Decimal
from typing import Dict, Optional, TypeVar

import a_sync
from generic_exporters.metric import Metric
from generic_exporters.processors._base import _ProcessorBase, _TimeSeriesProcessorBase
from generic_exporters.processors.exporters.datastores.timeseries._base import TimeSeriesDataStoreBase
from generic_exporters.processors.exporters.datastores.timeseries.sql import SQLTimeSeriesKeyValueStore


_T = TypeVar('_T')

class _ExporterBase(_ProcessorBase[None]):
    @abstractmethod
    async def data_exists(self) -> bool:
        """Returns True if data exists, False if it does not and must be produced."""


class _TimeSeriesExporterBase(_TimeSeriesProcessorBase, _ExporterBase):
    def __init__(self, metric: Metric, datastore: Optional[TimeSeriesDataStoreBase], sync: bool = True) -> None:
        super().__init__(sync=sync)
        self.metric = metric   
        if isinstance(datastore, TimeSeriesDataStoreBase):
            self.datastore = datastore
        elif datastore is None:
            self.datastore = SQLTimeSeriesKeyValueStore()
        else:
            raise TypeError(datastore)
        self.datastore = datastore


class _GatheringTimeSeriesProcessorBase(_TimeSeriesExporterBase):
    async def _gather(self) -> Dict[datetime, Decimal]:
        return await a_sync.gather({ts: self.metric.produce(ts, sync=False) async for ts in self._timestamps()})

class _PropertyExporterBase(_TimeSeriesExporterBase):
    output_type: _T
    @abstractproperty
    def property_name(self) -> str:
        pass
    @abstractmethod
    async def produce(self) -> _T:
        pass
