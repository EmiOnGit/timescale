from dataclasses import dataclass
from tslib.timeseries import Timeseries


@dataclass
class Alignment:
    translation: float
    scaling: float


def align(t1: Timeseries, t2: Timeseries) -> Alignment | None:
    pass
