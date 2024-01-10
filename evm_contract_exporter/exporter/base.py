

import asyncio
import logging
from abc import abstractmethod
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from functools import cached_property
from typing import TYPE_CHECKING, Optional

import a_sync
import eth_retry
from generic_exporters import TimeSeriesExporterBase
from tqdm.asyncio import tqdm_asyncio
from y import ERC20
from y.datatypes import Address
from y.contracts import contract_creation_block_async
from y.time import get_block_timestamp_async

from generic_exporters.datastores import SQLTimeSeriesKeyValueStore

from evm_contract_exporter import utils

if TYPE_CHECKING:
    from evm_contract_exporter.datastore import GenericContractTimeSeriesKeyValueStore


logger = logging.getLogger(__name__)

one_minute = 60
one_hour = one_minute * 60
deploy_block_semaphore = a_sync.Semaphore(50, "start_timestamp")

class ContractMetricTimeSeriesExporterBase(TimeSeriesExporterBase):
    datastore: "GenericContractTimeSeriesKeyValueStore"
    """A base class to adapt generic_exporter's TimeSeriesExporterBase for evm analysis needs. Inherit from this class to create your bespoke metric exporters."""
    _semaphore_value = 1
    def __init__(
        self, 
        contract: Address, 
        *, 
        interval: timedelta = timedelta(days=1), 
        buffer: timedelta = timedelta(minutes=5),
        datastore: Optional["GenericContractTimeSeriesKeyValueStore"] = None,
    ) -> None:
        self.address = contract
        self.semaphore = a_sync.Semaphore(self._semaphore_value, name=self.metric_name)
        assert datastore, 'you must provide a datastore.  # TODO make a default'
        super().__init__(interval, datastore, buffer)
    
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} address={self.address} token={self.address} semaphore={self.semaphore}>"

    @cached_property
    def erc20(self) -> ERC20:
        return ERC20(self.address, asynchronous=True)
    
    async def data_exists(self, ts) -> bool:
        return await self.datastore.data_exists(self.metric_name, ts)
    
    async def export(self) -> None:
        await tqdm_asyncio.gather(*[asyncio.create_task(self.ensure_data(ts)) async for ts in self.timestamps()], desc=str(self))
    
    @eth_retry.auto_retry
    async def start_timestamp(self) -> datetime:
        async with deploy_block_semaphore:
            deploy_block = await contract_creation_block_async(self.address)
        deploy_timestamp = datetime.fromtimestamp(await get_block_timestamp_async(deploy_block), tz=timezone.utc)
        # NOTE: round `deploy_timestamp` down according to `self.interval`
        if self.interval == timedelta(days=1):
            start_timestamp = datetime(year=deploy_timestamp.year, month=deploy_timestamp.month, day=deploy_timestamp.day, tzinfo=timezone.utc)
        elif self._interval_divisible_by_hour:
            start_timestamp = datetime(year=deploy_timestamp.year, month=deploy_timestamp.month, day=deploy_timestamp.day, hour=deploy_timestamp.hour, tzinfo=timezone.utc)
        elif self._interval_divisible_by_fifteen_minute:
            start_timestamp = datetime(year=deploy_timestamp.year, month=deploy_timestamp.month, day=deploy_timestamp.day, hour=deploy_timestamp.hour, minute=0 if (m := deploy_timestamp.minute) < 15 else 15 if m < 30 else 30 if m < 45 else 45, tzinfo=timezone.utc)
        elif self._interval_divisible_by_ten_minute:
            start_timestamp = datetime(year=deploy_timestamp.year, month=deploy_timestamp.month, day=deploy_timestamp.day, hour=deploy_timestamp.hour, minute=0 if (m := deploy_timestamp.minute) < 10 else 10 if m < 20 else 20 if m < 30 else 30 if m < 40 else 40 if m < 50 else 50, tzinfo=timezone.utc)
        elif self._interval_divisible_by_five_minute:
            start_timestamp = datetime(year=deploy_timestamp.year, month=deploy_timestamp.month, day=deploy_timestamp.day, hour=deploy_timestamp.hour, minute=0 if (m := deploy_timestamp.minute) < 5 else 5 if m < 10 else 10 if m < 15 else 15 if m < 20 else 20 if m < 25 else 25 if m < 30 else 30 if m < 35 else 35 if m < 40 else 40 if m < 45 else 45 if m < 50 else 50 if m < 55 else 55, tzinfo=timezone.utc)
        elif self._interval_divisible_by_minute:
            start_timestamp = datetime(year=deploy_timestamp.year, month=deploy_timestamp.month, day=deploy_timestamp.day, hour=deploy_timestamp.hour, minute=deploy_timestamp.minute, tzinfo=timezone.utc)
        else:
            raise ValueError(f'unsupported interval {self.interval}')
        logger.debug("rounded %s to %s (interval %s)", deploy_timestamp, start_timestamp, self.interval)
        return start_timestamp + self.interval
    
    async def produce(self, timestamp: datetime) -> Decimal:
        block = await utils.get_block_by_timestamp(timestamp)
        async with self.semaphore:
            logger.debug("%s producing %s block %s", self, timestamp, block)
            retval = await self._produce(block)
            logger.debug("%s produced %s at %s block %s", self, retval, timestamp, block)
            return retval

    @property
    def _interval_divisible_by_hour(self) -> bool:
        return self.interval.total_seconds() / one_hour == int(self.interval.total_seconds() / one_hour)

    @property
    def _interval_divisible_by_fifteen_minute(self) -> bool:
        return self.interval.total_seconds() / (one_minute * 15) == int(self.interval.total_seconds() / (one_minute * 15))
    
    @property
    def _interval_divisible_by_ten_minute(self) -> bool:
        return self.interval.total_seconds() / (one_minute * 10) == int(self.interval.total_seconds() / (one_minute * 10))
    
    @property
    def _interval_divisible_by_five_minute(self) -> bool:
        return self.interval.total_seconds() / (one_minute * 5) == int(self.interval.total_seconds() / (one_minute * 5))
    
    @property
    def _interval_divisible_by_minute(self) -> bool:
        return self.interval.total_seconds() / one_minute == int(self.interval.total_seconds() / one_minute)
    
    @abstractmethod
    async def _produce(self, block: int) -> Decimal:
        pass
