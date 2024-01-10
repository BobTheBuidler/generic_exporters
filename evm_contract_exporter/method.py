
import asyncio
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from functools import cached_property
from typing import Any, Optional, Union

import inflection
from a_sync.primitives.locks.prio_semaphore import PrioritySemaphore
from async_property import async_property
from brownie.network.contract import ContractCall, _ContractMethod
from y import ERC20

from generic_exporters.datastores.timeseries._base import TimeSeriesDataStoreBase
from generic_exporters.metric import Metric

from evm_contract_exporter import _exceptions, metric, utils
from evm_contract_exporter.exporter import ContractMetricTimeSeriesExporterBase


logger = logging.getLogger(__name__)

data_exists_semaphore = PrioritySemaphore(50, name="data_exists_semaphore")

class ContractCallMetric(ContractCall, Metric):
    def __init__(self, original_call: ContractCall, *args, scale: Union[bool, int, metric.Scale] = False) -> None:
        self._args = args
        self._scale = scale
        self._original_call = original_call
        super().__init__(original_call._address, original_call.abi, original_call._name, original_call._owner, original_call.natspec)
    @property
    def address(self) -> str:
        return self._address
    async def coroutine(self, *args, **kwargs) -> Any:
        """Maintains the async monkey-patching done by dank_mids on the original ContractCall object"""
        args = args or self._args
        return await self._original_call.coroutine(*args, **kwargs)
    async def process(self, timestamp: datetime) -> Decimal:
        block = await utils.get_block_by_timestamp(timestamp)
        retval = await self.coroutine(block_identifier=block)
        if self._scale:
            scale = await self.scale
            retval /= scale
        return retval
            
    @async_property
    async def scale(self) -> Optional[Decimal]:
        return None if self._scale is False else Decimal(await ERC20(self.address, asynchronous=True).scale) if self._scale is True else Decimal(self._scale)



class MethodMetricExporter(ContractMetricTimeSeriesExporterBase):
    _semaphore_value = 5000000 # effectively doesnt exist at this level
    def __init__(
        self, 
        method: _ContractMethod, 
        *,
        interval: timedelta = timedelta(days=1), 
        buffer: timedelta = timedelta(minutes=5), 
        scale: Union[bool, int] = False, 
        datastore: TimeSeriesDataStoreBase = None,
    ) -> None:
        self.method = method
        self.address = self.method._address
        super().__init__(self.method._address, interval=interval, datastore=datastore, buffer=buffer)
        if isinstance(scale, bool):
            pass
        elif not isinstance(scale, int):
            raise TypeError("scale must be an integer")
        elif not str(scale).endswith('00'): # NOTE: we assume tokens with decimal 1 are shit
            raise ValueError("you must provided the scaled decimal value, not the return value from decimals()")
        self._scale = scale
        if self._scale and not all(output["type"] == "uint256" for output in method.abi["outputs"]):
            self._scale = False
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} contract={self.address} method={self.method._name.split('.')[1]}>"
    @cached_property
    def metric_name(self) -> str:
        return inflection.underscore(self.method._name.split('.')[1])
    @async_property
    async def symbol(self) -> str:
        return await self.erc20.symbol
    @async_property
    async def name(self) -> str:
        return await self.erc20.name
    @async_property
    async def scale(self) -> Decimal:
        return Decimal(1) if self._scale is False else Decimal(await self.erc20.scale) if self._scale is True else Decimal(self._scale)
    async def ensure_data(self, ts: datetime) -> None:
        try:
            await super().ensure_data(ts)
        except Exception as e:
            if not _exceptions._is_revert(e):
                raise
            logger.debug("%s reverted with %s %s", self, e.__class__.__name__, e)
            
    async def data_exists(self, ts: datetime) -> bool:
        async with data_exists_semaphore[ts]:
            return await self.datastore.data_exists(self.metric_name, ts)
        
    async def _produce(self, block: int) -> Decimal:
        while True:
            try:
                value, scale = await asyncio.gather(self.method.coroutine(block_identifier=block), self.scale)
                return value / scale
            except Exception as e:
                if '429' not in str(e):
                    raise e
                await asyncio.sleep(1)
