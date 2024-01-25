import pytest
from decimal import Decimal
from generic_exporters.metric import Metric, Constant
from generic_exporters._mathable import _MathableBase

class DummyMathable(_MathableBase):
    def _validate_other(self, other):
        return other

    @property
    def __math_classes__(self):
        return (DummyMathable, DummyMathable, DummyMathable, DummyMathable, DummyMathable, DummyMathable)

@pytest.mark.asyncio
async def test_mathable_operations():
    dummy = DummyMathable()
    constant_five = Constant(5)

    # Test addition
    result = dummy + constant_five
    assert isinstance(result, DummyMathable)

    # Test subtraction
    result = dummy - constant_five
    assert isinstance(result, DummyMathable)

    # Test multiplication
    result = dummy * constant_five
    assert isinstance(result, DummyMathable)

    # Test true division
    result = dummy / constant_five
    assert isinstance(result, DummyMathable)

    # Test floor division
    result = dummy // constant_five
    assert isinstance(result, DummyMathable)

    # Test power
    result = dummy ** constant_five
    assert isinstance(result, DummyMathable)