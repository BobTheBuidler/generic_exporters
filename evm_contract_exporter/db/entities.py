

from datetime import datetime
from decimal import Decimal
from os import path

from pony.orm import Database, PrimaryKey, Required, Set, Optional, LongStr

from evm_contract_exporter.db.utils import db_session, write_threads


db = Database()

class Address(db.Entity):
    chainid = Required(int)
    address = Required(str)
    PrimaryKey(chainid, address)

    original_funding_source = Optional("Address", lazy=True)
    wallet_notes = Optional(LongStr, lazy=True)

    wallets_funded = Set("Address")
    contracts_deployed = Set("Contract")


class Contract(Address):
    deployer = Optional(Address)
    deploy_block = Optional(int, lazy=True)
    deploy_timestamp = Optional(datetime, lazy=True)
    contract_notes = Optional(LongStr, lazy=True)
    is_verified = Required(bool, default=False, index=True, lazy=True)
    last_checked_verified = Optional(datetime, index=True, lazy=True)

    @classmethod
    async def set_non_verified(cls, chainid: int, address: str) -> None:
        await write_threads.run(cls._set_non_verified, chainid, address)

    @classmethod
    @db_session
    def _set_non_verified(cls, chainid: int, address: str) -> None:
        entity = cls[chainid, address]
        entity.is_non_verified = True
        entity.last_checked_verified = datetime.utcnow()


class Token(Contract):
    name = Required(str, lazy=True)
    symbol = Required(str, index=True, lazy=True)
    token_notes = Optional(LongStr, lazy=True)


class ERC20(Token):
    decimals = Required(int)
    time_series_kv = Set("ERC20TimeSeriesKV", reverse="token")


class ContractDataTimeSeriesKV(db.Entity):
    token = Required(ERC20, reverse="time_series_kv")
    metric = Required(str)
    timestamp = Required(datetime)
    PrimaryKey(token, metric, timestamp)

    blockno = Required(int)
    value = Required(Decimal)


db.bind(
    provider = "sqlite",
    filename = f"{path.expanduser( '~' )}/.evm_contract_exporter/gce.sqlite",
    create_db = True,
)

db.generate_mapping(create_tables=True)
