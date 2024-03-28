
import asyncio
from abc import abstractmethod
from datetime import datetime, timedelta
from typing import Literal, NoReturn, Optional, Union, overload

import a_sync
from a_sync.utils.iterators import exhaust_iterator
from generic_exporters.plan import QueryPlan
from generic_exporters.processors.exporters._base import _TimeSeriesExporterBase
from generic_exporters.processors.exporters.datastores.timeseries._base import TimeSeriesDataStoreBase


class TimeSeriesExporter(_TimeSeriesExporterBase):
    """
    Inherit from this class to export the history of any `Metric` to a datastore of your choice.

    You must define a start_timestamp method that will determine the start of the historical range, and a data_exists method that determines whether or not the datastore already contains data for the `Metric` at a particular timestamp. This class will handle the rest.
    """
    def __init__(
        self, 
        query: QueryPlan, 
        datastore: TimeSeriesDataStoreBase, 
        *, 
        buffer: timedelta = timedelta(minutes=5), 
        concurrency: Optional[int] = None, 
        sync: bool = True,
    ) -> None:
        super().__init__(query, datastore, sync=sync)
        self.buffer = buffer
        self.concurrency = concurrency
    
    @abstractmethod
    async def data_exists(self, timestamp: datetime) -> bool:
        """Returns True if data exists at `timestamp`, False if it does not and must be exported."""

    @overload
    async def run(self, run_forever: Literal[True]) -> NoReturn:...
    @overload
    async def run(self, run_forever: Literal[False]) -> None:...
    async def run(self, run_forever: bool = False) -> Union[None, NoReturn]:
        """Exports the full history for this exporter's `Metric` to the datastore"""
        export_fn = lambda ts: self.ensure_data(ts, sync=False)
        await exhaust_iterator(a_sync.TaskMapping(export_fn, concurrency=self.concurrency).map(self.query._aiter_timestamps(run_forever)))

    async def ensure_data(self, ts: datetime) -> None:
        if not await self.data_exists(ts, sync=False):
            data = await self.query[ts]
            await asyncio.gather(*[self.datastore.push(key, ts, value) for key, value in data.items()])
