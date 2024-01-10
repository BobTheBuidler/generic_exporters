
from decimal import Decimal

from y import get_price

from evm_contract_exporter.exporter.base import ContractMetricTimeSeriesExporterBase


class PriceExporter(ContractMetricTimeSeriesExporterBase):
    metric_name = "price"
    async def _produce(self, block: int) -> Decimal:
        return Decimal(await get_price(self.address, block, skip_cache=True, silent=True, sync=False))
