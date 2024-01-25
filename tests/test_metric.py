import pytest
from decimal import Decimal
from datetime import datetime, timedelta
from generic_exporters.metric import Metric, Constant, _AdditionMetric, _SubtractionMetric, _MultiplicationMetric, _TrueDivisionMetric, _FloorDivisionMetric, _PowerMetric

class DummyMetric(Metric):
    async def produce(self, timestamp: datetime) -> Decimal:
        return Decimal(10)

    @property
    def key(self) -> str:
        return "dummy_metric"

@pytest.mark.asyncio
async def test_metric_not_implemented():
    metric = Metric()
    with pytest.raises(NotImplementedError):
        await metric.produce(datetime.utcnow())
    with pytest.raises(NotImplementedError):
        _ = metric.key

@pytest.mark.asyncio
async def test_constant_metric():
    value = Decimal(10)
    constant = Constant(value)
    assert constant.key == "constant"
    assert await constant.produce(datetime.utcnow()) == value

@pytest.mark.asyncio
async def test_addition_metric():
    metric1 = DummyMetric()
    metric2 = Constant(5)
    addition_metric = _AdditionMetric(metric1, metric2)
    result = await addition_metric.produce(datetime.utcnow())
    assert result == Decimal(15)

# Additional tests for subtraction, multiplication, division, etc. would follow a similar pattern.
# The following are placeholders for those tests.

@pytest.mark.asyncio
async def test_subtraction_metric():
    metric1 = DummyMetric()
    metric2 = Constant(5)
    subtraction_metric = _SubtractionMetric(metric1, metric2)
    result = await subtraction_metric.produce(datetime.utcnow())
    assert result == Decimal(5)

@pytest.mark.asyncio
async def test_multiplication_metric():
    metric1 = DummyMetric()
    metric2 = Constant(5)
    multiplication_metric = _MultiplicationMetric(metric1, metric2)
    result = await multiplication_metric.produce(datetime.utcnow())
    assert result == Decimal(50)

@pytest.mark.asyncio
async def test_true_division_metric():
    metric1 = DummyMetric()
    metric2 = Constant(5)
    true_division_metric = _TrueDivisionMetric(metric1, metric2)
    result = await true_division_metric.produce(datetime.utcnow())
    assert result == Decimal(2)

@pytest.mark.asyncio
async def test_floor_division_metric():
    metric1 = DummyMetric()
    metric2 = Constant(5)
    floor_division_metric = _FloorDivisionMetric(metric1, metric2)
    result = await floor_division_metric.produce(datetime.utcnow())
    assert result == Decimal(2)  # Assuming DummyMetric produces Decimal(10)

@pytest.mark.asyncio
async def test_power_metric():
    metric1 = DummyMetric()
    metric2 = Constant(2)
    power_metric = _PowerMetric(metric1, metric2)
    result = await power_metric.produce(datetime.utcnow())
    assert result == Decimal(100)
