
import asyncio
from abc import abstractmethod
from datetime import datetime, timedelta

from generic_exporters.metric import Metric
from generic_exporters.processors.exporters._base import _TimeSeriesExporterBase
from generic_exporters.processors.exporters.datastores.timeseries._base import TimeSeriesDataStoreBase


class TimeSeriesExporter(_TimeSeriesExporterBase):
    """
    Inherit from this class to export the history of any `Metric` to a datastore of your choice.

    You must define a start_timestamp method that will determine the start of the historical range, and a data_exists method that determines whether or not the datastore already contains data for the `Metric` at a particular timestamp. This class will handle the rest.
    """
    def __init__(self, metric: Metric, datastore: TimeSeriesDataStoreBase, interval: timedelta = timedelta(days=1), buffer: timedelta = timedelta(minutes=5), sync: bool = True) -> None:
        super().__init__(metric, datastore)
        self.interval = interval
        self.buffer = buffer

    @abstractmethod
    async def data_exists(self, timestamp: datetime) -> bool:
        """Returns True if data exists at `timestamp`, False if it does not and must be exported."""

    async def run(self) -> None:
        """Exports the full history for this exporter's `Metric` to the datastore"""
        await asyncio.gather(*[asyncio.create_task(self.ensure_data(ts, sync=False)) async for ts in self._timestamps()])

    async def ensure_data(self, ts: datetime) -> None:
        if not await self.data_exists(ts, sync=False):
            data = await self.metric.produce(ts, sync=False)
            await self.datastore.push(self.metric.key, ts, data)
