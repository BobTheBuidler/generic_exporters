
import logging
from abc import ABCMeta, abstractmethod
from datetime import timedelta

from brownie import convert

from evm_contract_exporter._exceptions import FixMe
from evm_contract_exporter.datastore import GenericContractTimeSeriesKeyValueStore
from evm_contract_exporter.types import address


logger = logging.getLogger(__name__)

class ContractExporterBase(metaclass=ABCMeta):
    def __init__(
        self, 
        contract: address, 
        chainid: int, 
        *, 
        interval: timedelta = timedelta(days=1), 
        buffer: timedelta = timedelta(minutes=5),
    ) -> None:
        self.chainid = chainid
        self.address = convert.to_address(contract)
        self.interval = interval
        self.buffer = buffer
        self.datastore = GenericContractTimeSeriesKeyValueStore(chainid, self.address)
    def __await__(self):
        try:
            return self._await().__await__()
        except FixMe as e:
            logger.warning('bob has to fix me: %s', e)
        except AttributeError:
            raise NotImplementedError
    @abstractmethod
    async def _await(self) -> None:
        ...
