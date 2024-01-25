
from abc import abstractmethod
from functools import cached_property
from typing import TYPE_CHECKING, Generic, Tuple, Type, TypeVar

import a_sync

from generic_exporters import _types

if TYPE_CHECKING:
    from generic_exporters.metric import _AdditionMetric, _SubtractionMetric, _MultiplicationMetric, _TrueDivisionMetric, _FloorDivisionMetric, _PowerMetric

_T = TypeVar('_T')

class _MathableBase(a_sync.ASyncGenericBase, Generic[_T]):
    def __add__(self, other: _types.Numericish) -> "_AdditionMetric":
        return self.__math_classes__[0](self, self._validate_other(other))
    def __sub__(self, other: _types.Numericish) -> "_SubtractionMetric":
        return self.__math_classes__[1](self, self._validate_other(other))
    def __mul__(self, other: _types.Numericish) -> "_MultiplicationMetric":
        return self.__math_classes__[2](self, self._validate_other(other))
    def __truediv__(self, other: _types.Numericish) -> "_TrueDivisionMetric":
        return self.__math_classes__[3](self, self._validate_other(other))
    def __floordiv__(self, other: _types.Numericish) -> "_FloorDivisionMetric":
        return self.__math_classes__[4](self, self._validate_other(other))
    def __pow__(self, other: _types.Numericish) -> "_PowerMetric":
        return self.__math_classes__[5](self, self._validate_other(other))
    @abstractmethod
    def _validate_other(self, other) -> _T:...
    @cached_property
    def __math_classes__(self) -> Tuple[Type[_T], Type[_T], Type[_T], Type[_T], Type[_T], Type[_T]]:
        """This just lets you subclass to do custom thingies"""
        from generic_exporters.metric import _AdditionMetric, _SubtractionMetric, _MultiplicationMetric, _TrueDivisionMetric, _FloorDivisionMetric, _PowerMetric
        return _AdditionMetric, _SubtractionMetric, _MultiplicationMetric, _TrueDivisionMetric, _FloorDivisionMetric, _PowerMetric