
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional

import a_sync
import eth_retry
from generic_exporters import TimeSeriesExporter
from y.contracts import contract_creation_block_async
from y.time import get_block_timestamp_async

from evm_contract_exporter import utils
from evm_contract_exporter.datastore import GenericContractTimeSeriesKeyValueStore
from evm_contract_exporter.metric import _AddressKeyedMetric
from evm_contract_exporter.types import address


logger = logging.getLogger(__name__)

deploy_block_semaphore = a_sync.Semaphore(100, "deploy block semaphore")

class ContractMetricExporter(TimeSeriesExporter):
    datastore: GenericContractTimeSeriesKeyValueStore
    """A base class to adapt generic_exporter's TimeSeriesExporterBase for evm analysis needs. Inherit from this class to create your bespoke metric exporters."""
    _semaphore_value = 1
    def __init__(
        self,
        chainid: int,
        contract: address,
        metric: _AddressKeyedMetric,
        *, 
        interval: timedelta = timedelta(days=1), 
        buffer: timedelta = timedelta(minutes=5),
        datastore: Optional["GenericContractTimeSeriesKeyValueStore"] = None,
        sync: bool = True,
    ) -> None:
        self.chainid = chainid
        self.address = contract
        datastore = datastore or GenericContractTimeSeriesKeyValueStore(self.chainid, self.address)
        super().__init__(metric, datastore, interval=interval, buffer=buffer, sync=sync)
        self.semaphore = a_sync.Semaphore(self._semaphore_value, name=self.timeseries.metric.key)
    
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} address={self.address} contracts={self.address}>"
    
    async def data_exists(self, ts) -> bool:
        return await self.datastore.data_exists(self.timeseries.metric.key, ts)
    
    @eth_retry.auto_retry
    async def start_timestamp(self) -> datetime:
        async with deploy_block_semaphore:
            deploy_block = await contract_creation_block_async(self.address)
        deploy_timestamp = datetime.fromtimestamp(await get_block_timestamp_async(deploy_block), tz=timezone.utc)
        iseconds = self.interval.total_seconds()
        start_timestamp = datetime.fromtimestamp(deploy_timestamp.timestamp() // iseconds * iseconds, tz=timezone.utc) + self.interval
        logger.debug("rounded %s to %s (interval %s)", deploy_timestamp, start_timestamp, self.interval)
        return start_timestamp
    
    async def produce(self, timestamp: datetime) -> Decimal:
        # NOTE: we fetch this before we enter the semaphore to ensure its cached in memory when we need to use it and we dont block unnecessarily
        block = await utils.get_block_at_timestamp(timestamp)
        async with self.semaphore:
            logger.debug("%s producing %s block %s", self, timestamp, block)
            retval = await self.timeseries.metric.produce(timestamp, sync=False)
            logger.debug("%s produced %s at %s block %s", self, retval, timestamp, block)
            return retval
