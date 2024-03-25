import copy
from types import FunctionType
from typing import Callable
from tslib.timeseries import Timeseries


class Pipeline:
    def __init__(self):
        self.fs = []

    def push(self, f: Callable[[Timeseries], Timeseries]):
        self.fs.append(f)

    def apply(self, ts: Timeseries, inplace=False):
        if not inplace:
            ts = copy.deepcopy(ts)
        for f in self.fs:
            ts = f(ts)
        return ts


def outlier_removal(ts: Timeseries):
    pass


def smoothing(ts: Timeseries):
    pass


def sampling(ts: Timeseries):
    pass


def segmentation(ts: Timeseries):
    pass


def power_transform(ts: Timeseries):
    pass


def difference_transform(ts: Timeseries):
    pass


def standardization(ts: Timeseries):
    pass


def normalization(ts: Timeseries):
    pass
