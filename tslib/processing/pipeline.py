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


def interpolate_int(factor):
    def _interpolate_apply(ts):
        df = ts.df
        time_c = df[ts._time_column]
        old_length = len(time_c)
        new_xs = np.linspace(time_c.iloc[0], time_c.iloc[-1], old_length * factor)
        df = df.reindex(range(old_length * factor))
        for c in df:
            df[c] = np.interp(new_xs, time_c, df[c][:old_length])
        ts.df = df
        return ts

    return _interpolate_apply


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
