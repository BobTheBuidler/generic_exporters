
import asyncio
from abc import abstractmethod
from functools import cached_property
from typing import TYPE_CHECKING, Awaitable, TypeVar

import a_sync

if TYPE_CHECKING:
    from generic_exporters.dataset import Dataset

_T = TypeVar('_T')

class _AwaitableMixin(a_sync.ASyncGenericBase, Awaitable[_T]):
    def __await__(self) -> _T:
        # NOTE: maybe put this in _Awaitable helper
        return self._task.__await__()
    @cached_property
    def _task(self) -> "asyncio.Task[Dataset[_T]]":
        """The task that executes the await logic"""
        return asyncio.create_task(self._materialize())  # TODO: name the task with some hueristic
    @abstractmethod
    def _materialize(self) -> _T:...