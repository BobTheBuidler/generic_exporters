
import logging
from datetime import datetime
from dateutil import parser
from decimal import Decimal, InvalidOperation
from typing import Any, List

from async_lru import alru_cache
from generic_exporters.datastores.timeseries._base import TimeSeriesDataStoreBase
from pony.orm import TransactionIntegrityError, select

from evm_contract_exporter import db, utils

logger = logging.getLogger(__name__)

class GenericContractTimeSeriesKeyValueStore(TimeSeriesDataStoreBase):
    def __init__(self, chain_id: int, token_address) -> None:
        self.chainid = chain_id
        self.token_address = token_address
        self.__integrity_errd = False  # we flip this true for token/method combos that have integrity err issues. TODO: debug this
    
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} chainid={self.chainid} token={self.token_address}>"
    
    @alru_cache(maxsize=None)
    async def timestamps_present(self, key: str) -> List[datetime]:
        timestamps = await db.read_threads.run(self._timestamps_present, key)
        logger.debug("%s timestamps found for %s %s", len(timestamps), self, key)
        return timestamps
    
    async def data_exists(self, key: str, ts: datetime) -> bool:
        """Returns True if `key` returns results from your Postgres db at `ts`, False if not."""
        if ts in await self.timestamps_present(key):
            logger.debug('%s %s %s %s exists', self.chainid, self.token_address, key, ts)
            return True
        logger.debug('%s %s %s %s does not exist', self.chainid, self.token_address, key, ts)
        return False
    
    async def push(self, key: Any, ts: datetime, value: Decimal) -> None:
        """Exports `data` to Victoria Metrics using `key` somehow. lol"""
        block = await utils.get_block_by_timestamp(ts)
        try:
            #await db.write_threads.run(self.__push, key, ts, block, value)
            await db.write_threads.run(
                db.session(db.ContractDataTimeSeriesKV), 
                token=(self.chainid, self.token_address), 
                metric=key, 
                timestamp=ts, 
                blockno=block, 
                value=value,
            )
            logger.info('exported {"key": %s, "ts": %s, "block": %s, "value": %s}', key, ts, block, value)
        except InvalidOperation as e:
            logger.debug("%s %s", e.__class__.__name__, e)
        except TransactionIntegrityError as e:
            if not self.__integrity_errd:
                logger.info("%s for %s %s %s", e.__class__.__name__, self, key, ts)
                self.__integrity_errd = True
    
    @db.session
    def _timestamps_present(self, key: str) -> List[datetime]:
        query = select(
            d.timestamp 
            for d in db.ContractDataTimeSeriesKV 
            if d.token.chainid == self.chainid 
            and d.token.address == self.token_address 
            and d.metric == key
        )
        return [parser.parse(datetimestr) for datetimestr in query]