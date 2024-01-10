

import asyncio
import logging
from datetime import timedelta
from typing import List

from async_property import async_cached_property
from brownie.exceptions import ContractNotFound
from brownie.network.contract import ContractCall, ContractTx, OverloadedMethod
from multicall.utils import raise_if_exception_in
from y import Contract, ContractNotVerified
from y.datatypes import Address

from evm_contract_exporter.contract import ContractExporterBase
from evm_contract_exporter.examples.price import PriceExporter
from evm_contract_exporter.method import MethodMetricExporter
from evm_contract_exporter.types import EXPORTABLE_TYPES, UNEXPORTABLE_TYPES


logger = logging.getLogger(__name__)

class GenericContractExporter(ContractExporterBase):
    """
    This exporter will export a full history of all of the contract's view methods which return a single numeric result.
    It will also export historical price data.
    """
    def __init__(
        self, 
        contract: Address, 
        chainid: int, 
        *, 
        interval: timedelta = timedelta(days=1), 
        buffer: timedelta = timedelta(minutes=5),
    ) -> None:
        super().__init__(contract, chainid, interval=interval, buffer=buffer)
        self.price_exporter = PriceExporter(contract, interval=interval, buffer=self.buffer, datastore=self.datastore)
    @async_cached_property
    async def contract(self) -> Contract:
        return await Contract.coroutine(self.address)
    @async_cached_property
    async def method_exporters(self) -> List[MethodMetricExporter]:
        contract = await self.contract
        return [
            MethodMetricExporter(
                method=view_method,
                interval=self.interval, 
                scale=True, 
                datastore=self.datastore,
            )
            for view_method in _safe_views(contract)
        ]
    async def _await(self) -> None:
        try:
            raise_if_exception_in(
                await asyncio.gather(*await self.method_exporters) #, return_exceptions=True) #, self.price_exporter) #, return_exceptions=True))
            )
        #except AttributeError as e:
        #    if "'NoneType' object has no attribute 'abi'" not in str(e):
        #        raise e
        #    logger.info("%s %s", e.__class__.__name__, e)
        except ContractNotFound:
            logger.debug("%s is not a contract", self)
        except ContractNotVerified:
            logger.debug("%s is not verified", self)
            raise
        except TypeError as e:
            logger.info("%s %s", e.__class__.__name__, e)

def _list_functions(contract: Contract) -> List[ContractCall]:
    fns = []
    for item in contract.abi:
        if item["type"] != "function":
            continue
        if fn := getattr(contract, item["name"]):
            if isinstance(fn, OverloadedMethod):
                fns.extend(_expand_overloaded(fn))
            elif isinstance(fn, (ContractCall, ContractTx)):
                fns.append(fn)
            else:
                raise TypeError(fn)
    return fns

def _is_view_method(function: ContractCall) -> bool:
    return function.abi.get("stateMutability") == "view"

def _list_view_methods(contract: Contract) -> List[ContractCall]:
    no_overloaded = [function for function in _list_functions(contract)]
    return [function for function in _list_functions(contract) if _is_view_method(function)]

def _expand_overloaded(fn: OverloadedMethod) -> List[ContractCall]:
    expanded = []
    for method in fn.methods.values():
        if isinstance(method, (ContractCall, ContractTx)):
            expanded.append(method)
        else:
            logger.info('we dont yet support %s %s', fn, method)
    assert all(isinstance(e, (ContractCall, ContractTx)) for e in expanded), expanded
    return expanded


def _has_no_args(function: ContractCall) -> bool:
    return not function.abi["inputs"]



SKIP_METHODS = {
    "decimals",
}

def _exportable_return_value_type(function: ContractCall) -> bool:
    name = function._name.split('.')[1]
    if name in SKIP_METHODS:
        return False
    outputs = function.abi["outputs"]
    if not outputs:
        return False
    if len(outputs) == 1:
        if (output_type := outputs[0]["type"]) in EXPORTABLE_TYPES:
            return True
        if output_type in UNEXPORTABLE_TYPES or output_type.endswith(']'):
            return False
    elif len(outputs) > 1:
        if all(o["type"] in UNEXPORTABLE_TYPES for o in outputs):
            return False
        elif all(o["name"] and o["type"] in EXPORTABLE_TYPES for o in outputs):
            logger.info('TODO: export struct types like %s %s', function, outputs)
            return False
    logger.info("cant export %s with outputs %s", function, outputs)
    return False

def _safe_views(contract: Contract) -> List[ContractCall]:
    """Returns a list of the view methods on `contract` that are suitable for exporting"""
    return [function for function in _list_view_methods(contract) if _has_no_args(function) and _exportable_return_value_type(function)]
