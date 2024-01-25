
import asyncio
import logging
from brownie import chain
from datetime import datetime
from dateutil import parser
from decimal import Decimal, InvalidOperation
from typing import Any, List, Optional

import a_sync
from async_lru import alru_cache
from generic_exporters import Metric
from generic_exporters.processors.exporters.datastores.timeseries._base import TimeSeriesDataStoreBase
from pony.orm import TransactionIntegrityError, select
from y import ERC20, NonStandardERC20
from y.prices.dex.uniswap.v2 import UniswapV2Pool

from evm_contract_exporter import db, types
from evm_contract_exporter._exceptions import FixMe
from evm_contract_exporter.utils import get_block_at_timestamp


logger = logging.getLogger(__name__)

class GenericContractTimeSeriesKeyValueStore(TimeSeriesDataStoreBase):
    def __init__(self, chain_id: int, address: types.address) -> None:
        self.chainid = chain_id
        self.address = address
        self.__errd = False  # we flip this true for token/method combos that have err issues. TODO: debug this
    
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} chainid={self.chainid} contract={self.address}>"
    
    @alru_cache(maxsize=None)
    async def timestamps_present(self, key: str) -> List[datetime]:
        await self._ensure_entity()
        timestamps = await db.read_threads.run(self._timestamps_present, key)
        logger.debug("%s timestamps found for %s %s", len(timestamps), self, key)
        return timestamps
    
    async def data_exists(self, key: str, ts: datetime) -> bool:
        """Returns True if `key` returns results from your Postgres db at `ts`, False if not."""
        if ts in await self.timestamps_present(key):
            logger.debug('%s %s %s %s exists', self.chainid, self.address, key, ts)
            return True
        logger.debug('%s %s %s %s does not exist', self.chainid, self.address, key, ts)
        return False
    
    async def push(self, key: Any, ts: datetime, value: Decimal, metric: Optional[Metric] = None) -> None:
        """Exports `data` to Victoria Metrics using `key` somehow. lol"""
        block = await get_block_at_timestamp(ts)
        try:
            #await db.write_threads.run(self.__push, key, ts, block, value)
            await db.write_threads.run(
                db.session(db.ContractDataTimeSeriesKV), 
                address=(self.chainid, self.address), 
                metric=key, 
                timestamp=ts, 
                blockno=block, 
                value=value,
            )
            logger.debug('exported {"key": %s, "ts": %s, "block": %s, "value": %s}', key, ts, block, value)
        except InvalidOperation as e:
            if not self.__errd:
                logger.info("%s %s (metric=%s,value=%s,scalevalue=%s)", e.__class__.__name__, e, metric, value, len(str(value).split('.')[0]))
                logger.info("bob will fix this before pushing the lib to prod")
                self.__errd = True
                raise FixMe(e) from None
        except TransactionIntegrityError as e:
            constraint_errs = "UNIQUE constraint failed", "duplicate key value violates unique constraint"
            if not any(err in (msg:=str(e)) for err in constraint_errs):
                raise e
            logger.debug("%s for %s %s %s", e.__class__.__name__, self, key, ts)
    
    @db.session
    def _timestamps_present(self, key: str) -> List[datetime]:
        query = select(
            d.timestamp 
            for d in db.ContractDataTimeSeriesKV 
            if d.address.chainid == self.chainid 
            and d.address.address == self.address 
            and d.metric == key
        )
        return [parser.parse(datetimestr) for datetimestr in query]
    
    async def _ensure_entity(self) -> None:
        if await db.read_threads.run(db.Contract.entity_exists, chain.id, self.address):
            return
        
        kwargs = {'chainid': chain.id, 'address': self.address}
        try:
            async with _entity_semaphore:
                erc20 = ERC20(self.address, asynchronous=True)
                name, symbol, decimals = await asyncio.gather(erc20.name, erc20.symbol, erc20.decimals)
                if await (pool:=UniswapV2Pool(self.address, asynchronous=True)).is_uniswap_pool():
                    token0, token1 = await asyncio.gather(pool.token0, pool.token1)
                    if not isinstance(token0, ERC20):
                        token0 = ERC20(token0, asynchronous=True)
                    if not isinstance(token1, ERC20):
                        token1 = ERC20(token1, asynchronous=True)
                    token0_symbol, token1_symbol = await asyncio.gather(token0.symbol, token1.symbol)
                    extra = f" ({token0_symbol}/{token1_symbol})"
                    name += extra
                    symbol += extra
                await db.write_threads.run(db.ERC20.insert_entity, **kwargs, name=name, symbol=symbol, decimals=decimals)
        except NonStandardERC20 as e:
            if 'decimals' in str(e):
                # Could be a NFT
                # TODO: implement this but raise for now..  with suppress(NonStandardERC20):
                kwargs['name'], kwargs['symbol'] = await asyncio.gather(erc20.name, erc20.symbol)
                await db.write_threads.run(db.Token.insert_entity, **kwargs)
                return
            raise
            await db.write_threads.run(db.Contract.insert_entity, **kwargs)
        except AssertionError as e:
            if 'probe' not in str(e):
                if str(e) != 'uint112,uint112':  # this just addresses an easter egg that needs fixin' in ypricemagic
                    raise
            logger.info('investigate probe issue %s', self)
        except Exception as e:
            logger.info('%s when ensuring entity for %s: %s', e.__class__.__name__, self, e)

_entity_semaphore = a_sync.Semaphore(5_000, name='evm_contract_exporter entity semaphore')
