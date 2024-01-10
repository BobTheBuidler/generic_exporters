
from abc import ABCMeta, abstractmethod
from datetime import timedelta

from y.datatypes import Address

from evm_contract_exporter.datastore import GenericContractTimeSeriesKeyValueStore

class ContractExporterBase(metaclass=ABCMeta):
    def __init__(
        self, 
        contract: Address, 
        chainid: int, 
        *, 
        interval: timedelta = timedelta(days=1), 
        buffer: timedelta = timedelta(minutes=5),
    ) -> None:
        self.chainid = chainid
        self.address = contract
        self.interval = interval
        self.buffer = buffer
        self.datastore = GenericContractTimeSeriesKeyValueStore(chainid, self.address)
    def __await__(self):
        try:
            return self._await().__await__()
        except AttributeError:
            raise NotImplementedError
    @abstractmethod
    async def _await(self) -> None:
        ...
