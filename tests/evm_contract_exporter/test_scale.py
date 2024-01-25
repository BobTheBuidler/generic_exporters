
from decimal import Decimal

from fixtures import *

test_val = Decimal(10 ** 18)

def test_scale(scale):
    assert scale.value == test_val
    assert scale.produce(None) == test_val

def test_smart_scale(smart_scale):
    assert smart_scale.produce(None) == test_val
