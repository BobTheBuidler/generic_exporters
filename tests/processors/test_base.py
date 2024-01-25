
import pytest
from datetime import datetime, timedelta
from generic_exporters.processors._base import _ProcessorBase, _TimeSeriesProcessorBase

class DummyProcessor(_ProcessorBase):
    async def run(self):
        pass

class DummyTimeSeriesProcessor(_TimeSeriesProcessorBase):
    interval = timedelta(minutes=5)
    async def start_timestamp(self) -> datetime:
        return datetime(2020, 1, 1)
    async def run(self):
        pass

def test_processor_base_not_implemented():
    processor = DummyProcessor()
    assert processor is not None

@pytest.mark.asyncio
async def test_time_series_processor_base_timestamps():
    processor = DummyTimeSeriesProcessor()
    timestamps = [ts async for ts in processor._timestamps()]
    assert timestamps[0] == datetime(2020, 1, 1)
    assert timestamps[1] == datetime(2020, 1, 1, 0, 5)