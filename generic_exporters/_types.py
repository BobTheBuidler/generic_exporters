
from decimal import Decimal
from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from generic_exporters import Metric, TimeSeries
    from generic_exporters.timeseries import _TimeSeriesBase

Numeric = Union[int, float, Decimal]
Numericish = Union[Numeric, "Metric"]
Processable = Union["Metric", "_TimeSeriesBase"]
SingleProcessable = Union["Metric", "TimeSeries"]
