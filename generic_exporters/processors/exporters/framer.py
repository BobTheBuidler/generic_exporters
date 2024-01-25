
from datetime import timedelta

from async_lru import alru_cache
from pandas import DataFrame

from generic_exporters.metric import Metric
from generic_exporters.processors.exporters._base import _GatheringTimeSeriesProcessorBase


class DataFramer(_GatheringTimeSeriesProcessorBase):
    """
    Inherit from this class to plot any `Metric` on a line chart.

    You must define a start_timestamp method that will determine the start of the historical range, and a data_exists method that determines whether or not the datastore already contains data for the `Metric` at a particular timestamp. This class will handle the rest.
    """
    def __init__(self, metric: Metric, interval: timedelta = timedelta(days=1), buffer: timedelta = timedelta(minutes=5), sync: bool = True) -> None:
        super().__init__(metric)
        self.interval = interval
        self.buffer = buffer

    @alru_cache
    async def run(self) -> DataFrame:
        """Exports the full history for this exporter's `Metric` to the datastore"""
        return DataFrame(await self._gather)
