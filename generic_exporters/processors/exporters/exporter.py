
import asyncio
from abc import abstractmethod
from datetime import datetime, timedelta
from typing import List

from generic_exporters.plan import QueryPlan
from generic_exporters.processors.exporters._base import _TimeSeriesExporterBase
from generic_exporters.processors.exporters.datastores.timeseries._base import TimeSeriesDataStoreBase


class TimeSeriesExporter(_TimeSeriesExporterBase):
    """
    Inherit from this class to export the history of any `Metric` to a datastore of your choice.

    You must define a start_timestamp method that will determine the start of the historical range, and a data_exists method that determines whether or not the datastore already contains data for the `Metric` at a particular timestamp. This class will handle the rest.
    """
    def __init__(self, query: QueryPlan, datastore: TimeSeriesDataStoreBase, *, buffer: timedelta = timedelta(minutes=5), sync: bool = True) -> None:
        super().__init__(query, datastore, sync=sync)
        self.buffer = buffer
    
    @abstractmethod
    async def data_exists(self, timestamp: datetime) -> bool:
        """Returns True if data exists at `timestamp`, False if it does not and must be exported."""

    async def run(self, run_forever: bool = False) -> None:
        """Exports the full history for this exporter's `Metric` to the datastore"""
        tasks: List[asyncio.Task] = []
        async for ts in self.query._aiter_timestamps(run_forever):
            tasks.append(asyncio.create_task(self.ensure_data(ts, sync=False)))
            await self._prune_running(tasks)
        # runs if `run_forever` is False
        for t in asyncio.as_completed(tasks):
            await t

    async def ensure_data(self, ts: datetime) -> None:
        if not await self.data_exists(ts, sync=False):
            data = await self.query[ts]
            await asyncio.gather(*[self.datastore.push(key, ts, value) for key, value in data.items()])

    async def _prune_running(self, running_tasks: List[asyncio.Task]) -> None:
        for t in running_tasks[:]:
            if t.done():
                await t
                running_tasks.remove(t)
