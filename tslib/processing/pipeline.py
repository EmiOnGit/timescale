import copy
from typing import Callable
from tslib.timeseries import Timeseries
import numpy as np


class Pipeline:
    def __init__(self):
        self.fs = []

    def push(self, f: Callable[[Timeseries], Timeseries]):
        self.fs.append(f)
        return self

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


def add(n=1):
    def adder(ts: Timeseries):
        df = ts.df
        df.loc[:, df.columns != ts._time_column] = df.loc[
            :, df.columns != ts._time_column
        ].apply(lambda x: x + n)
        return ts

    return adder


def normalization(ts: Timeseries):
    df = ts.df
    df.loc[:, df.columns != ts._time_column] = df.loc[
        :, df.columns != ts._time_column
    ].apply(lambda x: (x - np.min(x)) / (np.max(x) - np.min(x)))
    return ts
