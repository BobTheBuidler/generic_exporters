
import decimal
import logging
from datetime import datetime, timedelta
from typing import Union

from brownie import chain
from brownie.network.contract import _ContractMethod

from generic_exporters.processors.exporters.datastores.timeseries._base import TimeSeriesDataStoreBase

from evm_contract_exporter import _exceptions
from evm_contract_exporter.exporter.base import ContractMetricExporter
from evm_contract_exporter.metric import ContractCallDerivedMetric, ContractCallMetric
from evm_contract_exporter.scale import Scale


REVERT = -1

logger = logging.getLogger(__name__)

class ViewMethodExporter(ContractMetricExporter):
    """Used to export all view methods on a contract that return a numeric or boolean value"""
    metric: Union[ContractCallMetric, ContractCallDerivedMetric] 
    _semaphore_value = 500_000 # effectively doesnt exist at this level # TODO: dev something so we can make this None
    def __init__(
        self, 
        method: Union[_ContractMethod, ContractCallMetric, ContractCallDerivedMetric],
        *,
        interval: timedelta = timedelta(days=1), 
        buffer: timedelta = timedelta(minutes=5), 
        scale: Union[bool, int, Scale] = False, 
        datastore: TimeSeriesDataStoreBase = None,
        sync: bool = True,
    ) -> None:
        metric = method if isinstance(method, (ContractCallMetric, ContractCallDerivedMetric)) else ContractCallMetric(method, scale=scale)
        super().__init__(chain.id, metric.address, metric, interval=interval, datastore=datastore, buffer=buffer, sync=sync)
        if isinstance(scale, bool):
            pass
        elif not isinstance(scale, int):
            raise TypeError("scale must be an integer")
        elif not str(scale).endswith('00'): # NOTE: we assume tokens with decimal 1 are shit
            raise ValueError("you must provided the scaled decimal value, not the return value from decimals()")
        self._scale = scale
        try:
            if self._scale and not all(output["type"] == "uint256" for output in method.abi["outputs"]):
                self._scale = False
        except AttributeError:
            self._scale = False
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} contract={self.address} method={self.metric._name}>"
    async def ensure_data(self, ts: datetime) -> None:
        try:
            if not await self.data_exists(ts, sync=False):
                async with self.semaphore:
                    # make sure insert threads dont get backlogged
                    data = await self.metric.produce(ts, sync=False)
                    await self.datastore.push(self.metric.key, ts, data, self)
        except Exception as e:
            if not _exceptions._is_revert(e):
                raise e
            logger.debug("%s reverted with %s %s", self, e.__class__.__name__, e)
            await self.datastore.push(self.metric.key, ts, REVERT, self)
    async def data_exists(self, ts: datetime) -> bool:
        return await self.datastore.data_exists(self.metric.key, ts)
