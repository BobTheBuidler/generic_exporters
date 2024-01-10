

import asyncio
from abc import abstractmethod, abstractproperty
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import AsyncGenerator

from generic_exporters.datastores.timeseries._base import TimeSeriesDataStoreBase
from generic_exporters.exporters.base import _MetricExporterABC


class TimeSeriesExporterBase(_MetricExporterABC):
    def __init__(self, interval: timedelta, datastore: TimeSeriesDataStoreBase, buffer: timedelta = timedelta(minutes=5)) -> None:
        super().__init__(datastore)
        self.interval = interval
        self.buffer = buffer
    
    def __await__(self):
        return self.export().__await__()

    @abstractmethod
    async def data_exists(self, timestamp: datetime) -> bool:
        """Returns True if data exists at `timestamp`, False if it does not and must be exported."""

    @abstractmethod
    def produce(self, timestamp: datetime) -> Decimal:
        pass

    @abstractmethod
    async def start_timestamp(self) -> datetime:
        pass

    async def export(self) -> None:
        await asyncio.gather(*[asyncio.create_task(self.ensure_data(ts)) async for ts in self.timestamps()])

    async def ensure_data(self, ts: datetime) -> None:
        if not await self.data_exists(ts):
            data = await self.produce(ts)
            await self.datastore.push(self.metric_name, ts, data)
    
    async def timestamps(self) -> AsyncGenerator[datetime, None]:
        timestamp = await self.start_timestamp()
        while timestamp < datetime.now(tz=timezone.utc) - self.interval:
            yield timestamp
            timestamp += self.interval
