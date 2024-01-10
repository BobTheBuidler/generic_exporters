
from generic_exporters.metric import Constant

class Scale(Constant):
    def __init__(self, decimals: int) -> None:
        super().__init__(10 ** decimals)
