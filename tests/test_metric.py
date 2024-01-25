
from datetime import datetime, timedelta
from decimal import Decimal

import pytest
from generic_exporters import Constant, Metric, TimeSeries

    
class Two(Metric):
    key = "two"
    def produce(self, timestamp: datetime) -> Decimal:
        return 2
    
class Three(Metric):
    key = "three"
    def produce(self, timestamp: datetime) -> Decimal:
        return 3
    
def test_repr():
    assert Two().__repr__() == f'<Two key=two>'

def test_slice():
    two = Two()
    with pytest.raises(KeyError):
        two[1]
    with pytest.raises(TypeError):
        two[1:2]
    with pytest.raises(TypeError):
        two[1:2:3]

    assert isinstance(two[datetime.utcnow(): datetime.utcnow()], TimeSeries)
    assert isinstance(two[datetime.utcnow(): datetime.utcnow(): timedelta(minutes=1)], TimeSeries)

    with pytest.raises(ValueError):
        two[datetime.utcnow(): datetime.utcnow() + timedelta(minutes=1)]


def test_add():
    assert (Two() + Three()).produce(datetime.utcnow(), sync=True) == 5
    
def test_sub():
    assert (Two() - Three()).produce(datetime.utcnow(), sync=True) == -1

def test_mul():
    assert (Two() * Three()).produce(datetime.utcnow(), sync=True) == 6

def test_truediv():
    assert (Two() / Three()).produce(datetime.utcnow(), sync=True) == 2/3

def test_floordiv():
    assert (Two() // Three()).produce(datetime.utcnow(), sync=True) == 0

def test_power():
    assert (Two() ** Three()).produce(datetime.utcnow(), sync=True) == 8

def test_constant():
    assert Constant(5) is Constant(5)
    assert Constant(5).produce(datetime.utcnow(), sync=True) == 5