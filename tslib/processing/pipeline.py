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


def interpolate(factor):
    """Interpolates all data points"""

    def _interpolate_apply(ts: Timeseries):
        df = ts.df
        # number of elements in each column after interpolating
        num = int(len(df) * factor)
        # x2_old = [x for x in range(len(df))]
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
    """overrides the `time_column` of the time series with the index of the `DataFrame`

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


def segmentation(ts: Timeseries):
    pass


def power_transform(ts: Timeseries):
    pass


def difference_transform(ts: Timeseries):
    pass


def standardization(ts: Timeseries):
    pass


def mult(n=1.0):
    def multiplier(ts: Timeseries):
        df = ts.data_df()
        for c in df:
            x = df[c]
            ts.df[c] = x * n
        return ts

    return multiplier


def add(n=1.0):
    def adder(ts: Timeseries):
        df = ts.data_df()
        for c in df:
            x = df[c]
            ts.df[c] = x + n
        return ts

    return adder


def normalization(min=0.0, max=1.0):
    def normalize(ts: Timeseries):
        df = ts.data_df()
        for c in df:
            x = df[c]
            ts.df[c] = ((x - np.min(x)) / (np.max(x) - np.min(x))) * (max - min) + min
        return ts

    return normalize
