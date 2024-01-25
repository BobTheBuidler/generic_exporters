import pytest
from datetime import datetime, timedelta
from decimal import Decimal
from generic_exporters.timeseries import TimeSeries
from generic_exporters.metric import Metric

class DummyMetric(Metric):
    async def produce(self, timestamp: datetime) -> Decimal:
        return Decimal(10)
    @property
    def key(self) -> str:
        return "dummy_metric"

def test_timeseries_initialization():
    metric = DummyMetric()
    start_timestamp = datetime.utcnow()
    interval = timedelta(minutes=1)
    timeseries = TimeSeries(metric, start_timestamp, None, interval)
    assert timeseries.metric == metric
    assert timeseries.start_timestamp == start_timestamp
    assert timeseries.interval == interval
