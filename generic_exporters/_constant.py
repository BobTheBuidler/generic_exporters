
import threading
from collections import defaultdict
from typing import Any, DefaultDict, Dict, Tuple, TypeVar

from a_sync._meta import ASyncMeta


_T = TypeVar('_T')

class ConstantSingletonMeta(ASyncMeta):
    """NOTE: threadsafe"""
    def __init__(cls, name: str, bases: Tuple[type, ...], namespace: Dict[str, Any]) -> None:
        cls.__instances: DefaultDict[_T, Dict[bool, object]] = defaultdict(dict)
        cls.__lock = threading.Lock()
        super().__init__(name, bases, namespace)

    def __call__(cls, value: _T):
        is_sync = cls.__a_sync_instance_will_be_sync__((value), {})  # type: ignore [attr-defined]
        if is_sync not in cls.__instances[value]:
            with cls.__lock:
                # Check again in case `__instance` was set while we were waiting for the lock.
                if is_sync not in cls.__instances[value]:
                    cls.__instances[value][is_sync] = super().__call__(value)
        return cls.__instances[value][is_sync]
