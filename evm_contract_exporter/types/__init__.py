
from evm_contract_exporter.types.int import *
from evm_contract_exporter.types.uint import *

class address(str):
    ...

class bytes32(bytes):
    ...

EXPORTABLE_TYPES = {
    "bool": bool,
    "int8": int8,
    "int16": int16,
    "int24": int24,
    "int32": int32,
    "int64": int64,
    "int96": int96,
    "int112": int112,
    "int128": int128,
    "int192": int192,
    "int256": int256,
    "uint8": uint8,
    "uint16": uint16,
    "uint24": uint24,
    "uint32": uint32,
    "uint64": uint64,
    "uint96": uint96,
    "uint112": uint112,
    "uint128": uint128,
    "uint192": uint192,
    "uint256": uint256,
}

UNEXPORTABLE_TYPES = {
    "string": str,
    "bytes": bytes,
    "bytes32": bytes32,
    "address": address,
}
