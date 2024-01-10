
import asyncio
from abc import ABCMeta, abstractmethod, abstractproperty
from datetime import datetime
from decimal import Decimal
from typing import Union


class Metric(metaclass=ABCMeta):
    def __add__(self, other: Union[int, "Metric"]) -> "AdditionMetric":
        if isinstance(other, int):
            other = Constant(other)
        if not isinstance(other, Metric):
            raise TypeError(other)
        return AdditionMetric(self, other)
    def __sub__(self, other: Union[int, "Metric"]) -> "SubtractionMetric":
        if isinstance(other, int):
            other = Constant(other)
        if not isinstance(other, Metric):
            raise TypeError(other)
        return SubtractionMetric(self, other)
    def __mul__(self, other: Union[int, "Metric"]) -> "MultiplicationMetric":
        if isinstance(other, int):
            other = Constant(other)
        if not isinstance(other, Metric):
            raise TypeError(other)
        return MultiplicationMetric(self, other)
    def __truediv__(self, other: Union[int, "Metric"]) -> "TrueDivisionMetric":
        if isinstance(other, int):
            other = Constant(other)
        if not isinstance(other, Metric):
            raise TypeError(other)
        return TrueDivisionMetric(self, other)
    def __floordiv__(self, other: Union[int, "Metric"]) -> "FloorDivisionMetric":
        if isinstance(other, int):
            other = Constant(other)
        if not isinstance(other, Metric):
            raise TypeError(other)
        return FloorDivisionMetric(self, other)
    def __pow__(self, other: Union[int, "Metric"]) -> "PowerMetric":
        if isinstance(other, int):
            other = Constant(other)
        if not isinstance(other, Metric):
            raise TypeError(other)
        return PowerMetric(self, other)
    @abstractmethod
    async def process(self, timestamp: datetime) -> Decimal:
        ...



class Constant(Metric):  # TODO: make this a singleton
    def __init__(self, value: Union[int, Decimal]) -> None:
        if not isinstance(value, (int, Decimal)):
            raise TypeError(value)
        self.value = value
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} value={self.value}>"
    async def process(self, timestamp: datetime) -> Decimal:
        return self.value



class _ArithmeticResultMetricBase(Metric):
    def __init__(self, metric0: Metric, metric1: Metric) -> None:
        self.metric0 = metric0
        self.metric1 = metric1
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} for {self.metric0} {self._symbol} {self.metric1}>"
    @abstractproperty
    def _symbol(self) -> str:
        ...


class AdditionMetric(_ArithmeticResultMetricBase):
    _symbol = '+'
    async def process(self, timestamp: datetime) -> Decimal:
        result0, result1 = await asyncio.gather(self.metric0.process(timestamp), self.metric1.process(timestamp))
        return result0 + result1
    
class SubtractionMetric(_ArithmeticResultMetricBase):
    _symbol = '-'
    async def process(self, timestamp: datetime) -> Decimal:
        result0, result1 = await asyncio.gather(self.metric0.process(timestamp), self.metric1.process(timestamp))
        return result0 - result1
    
class MultiplicationMetric(_ArithmeticResultMetricBase):
    _symbol = '*'
    async def process(self, timestamp: datetime) -> Decimal:
        result0, result1 = await asyncio.gather(self.metric0.process(timestamp), self.metric1.process(timestamp))
        return result0 * result1
    
class TrueDivisionMetric(_ArithmeticResultMetricBase):
    _symbol = '/'
    async def process(self, timestamp: datetime) -> Decimal:
        result0, result1 = await asyncio.gather(self.metric0.process(timestamp), self.metric1.process(timestamp))
        return result0 / result1
    
class FloorDivisionMetric(_ArithmeticResultMetricBase):
    _symbol = '//'
    async def process(self, timestamp: datetime) -> Decimal:
        result0, result1 = await asyncio.gather(self.metric0.process(timestamp), self.metric1.process(timestamp))
        return result0 // result1
    
class PowerMetric(_ArithmeticResultMetricBase):
    _symbol = '**'
    async def process(self, timestamp: datetime) -> Decimal:
        result0, result1 = await asyncio.gather(self.metric0.process(timestamp), self.metric1.process(timestamp))
        return result0 ** result1

