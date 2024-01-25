import pytest
from datetime import datetime
from decimal import Decimal

from generic_exporters.processors.exporters.datastores.timeseries.sql import SQLTimeSeriesKeyValueStore

@pytest.fixture
def sql_data_store():
    return SQLTimeSeriesKeyValueStore()

@pytest.mark.asyncio
async def test_sql_data_store_methods(sql_data_store):
    key = 'test_key'
    ts = datetime.utcnow()
    data = Decimal(10)
    exists = await sql_data_store.data_exists(key, ts)
    assert exists is True
    await sql_data_store.push(key, ts, data)  # No assert needed, just checking for exceptions