import pytest
from datetime import datetime, timedelta
from generic_exporters.processors._base import _TimeSeriesProcessorBase

from fixtures import *
class DummyTimeSeriesProcessor(_TimeSeriesProcessorBase):
    async def start_timestamp(self):
        return datetime.now() - timedelta(days=1)

@pytest.mark.asyncio
async def test_time_series_processor_base(time_series):
    processor = DummyTimeSeriesProcessor(time_series)
    assert processor.timeseries == time_series

    # Test _timestamps generator
    start_timestamp = await processor.start_timestamp()
    timestamps = [ts async for ts in processor._timestamps()]
    assert timestamps[0] == start_timestamp