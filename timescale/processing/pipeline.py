from __future__ import annotations
import copy
from typing import Callable, Iterable, List
import pandas as pd

from timescale.timeseries import Timeseries
import numpy as np


class Pipeline:
    def __init__(self):
        self.fs: List[Callable[[Timeseries], Timeseries]] = []

    def push(self, f: Callable[[Timeseries], Timeseries]) -> Pipeline:
        self.fs.append(f)
        return self

    def push_batch(self, fs: Iterable[Callable[[Timeseries], Timeseries]]) -> Pipeline:
        for f in fs:
            self = self.push(f)
        return self

    def apply(self, ts: Timeseries, inplace=False):
        if not inplace:
            ts = copy.deepcopy(ts)
        for f in self.fs:
            ts = f(ts)
        return ts

    def __repr__(self) -> str:
        fs = "[" + " | ".join([f.__name__ for f in self.fs]) + "]"
        return "Pipeline: " + fs

    def __call__(self, ts: Timeseries, inplace=False) -> Timeseries:
        return self.apply(ts, inplace=inplace)

    def pop(self):
        self.fs.pop()

    def copy(self) -> Pipeline:
        pipeline = Pipeline()
        for f in self.fs:
            pipeline.push(f)
        return pipeline


def outlier_removal(ts: Timeseries):
    pass


def smoothing(ts: Timeseries):
    pass


def sampling(ts: Timeseries):
    pass


def interpolate(factor):
    """Interpolates all data points by a given factor.
    If the `Timerseries` has 10 datapoints and factor=1.5, then the returned timeseries will have 15 datapoints.
    The amount will be rounded to the next integer.
    Linear interpolation is used.
    """

    def _interpolate_apply(ts: Timeseries):
        df = ts.df
        # number of elements in each column after interpolating
        num = int(len(df) * factor)
        x2_old = [x for x in ts.time_column()]
        # The x axis should be in the same interval than before
        x2 = np.linspace(x2_old[0], x2_old[-1], num=num)
        # We need to first calculate all interpolation before reindexing because weird sideeffects might occur if the df was indexed weirdly before
        interp = {}
        for c in df:
            interp[c] = np.interp(x2, x2_old, df[c])
        df = df.reindex(range(len(x2)))
        for c in df:
            df[c] = interp[c]
        ts.df = df
        return ts

    return _interpolate_apply


def index_to_time(ts: Timeseries):
    """Overrides the `time_column` of the `Timeseries` with the current index of the `DataFrame`.

    ---
    Examples:
    ```python
    df = pd.DataFrame(data={"ticks": [1, 2, 3], "data": [-1.0, 4.0, 9.0]})
    df.set_index(pd.Index([2, 3, 4]), inplace=True)
    ts = Timeseries(df, time_column="ticks")
    pipeline = Pipeline().push(index_to_time)
    ts = pipeline.apply(ts)
    assert all(ts.time_column() == [2, 3, 4])
    ```
    """
    ts.df[ts._time_column] = [x for x in ts.df.index]
    return ts


def cut_front(n=1, reindex=False):
    def cut_front_inner(ts: Timeseries) -> Timeseries:
        ts.df = pd.DataFrame(ts.df[n:].dropna(axis="rows"))
        if reindex:
            ts.df.index = ts.df.index - n
        return ts

    return cut_front_inner


def segmentation(ts: Timeseries):
    pass


def power_transform(ts: Timeseries):
    pass


def difference_transform(ts: Timeseries):
    pass


def standardization(ts: Timeseries):
    pass


def mult(x=1.0):
    """Multiplies each row with x.
    This does not affect the _time_column of the `Timeseries`.
    """

    def multiplier(ts: Timeseries):
        df = ts.data_df()
        for c in df:
            r = df[c]
            ts.df[c] = r * x
        return ts

    return multiplier


def add(x=1.0):
    """Adds x to each row.
    This does not affect the _time_column of the `Timeseries`.
    """

    def adder(ts: Timeseries):
        df = ts.data_df()
        for c in df:
            r = df[c]
            ts.df[c] = r + x
        return ts

    return adder


def normalization(min=0.0, max=1.0):
    """Normalizes each row of the `Timeseries to be between `min` and `max`.
    This means that for each data row `x`:
        min(x) == min
        max(x) == max
    Normalization does not affter the _time_column of the `Timeseries`
    """

    def normalize(ts: Timeseries):
        df = ts.data_df()
        for c in df:
            x = df[c]
            ts.df[c] = ((x - np.min(x)) / (np.max(x) - np.min(x))) * (max - min) + min
        return ts

    return normalize
