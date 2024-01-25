
import pytest
from generic_exporters._time import _TimeDataBase
from generic_exporters.metric import Metric

class DummyTimeData(_TimeDataBase):
    pass

@pytest.mark.asyncio
async def test_time_data_base_init():
    metric = Metric()
    fields = [metric]
    time_data = DummyTimeData(fields)
    assert time_data.fields == fields