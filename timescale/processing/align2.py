from timescale.processing.pipeline import (
    Pipeline,
    normalization,
    interpolate_count,
    index_to_time,
)
import pandas as pd
import numpy as np
from timescale.timeseries import Timeseries


def mse(ts1: Timeseries, ts2: Timeseries) -> float:
    """mean square error or two timeseries.
    The timeseries are zipped row-wise"""
    df1 = ts1.data_df()
    df2 = ts2.data_df()
    c_sum = 0.0
    n = min(len(df1), len(df2))
    amount = 0
    for c in zip(df1, df2):
        c_sum += np.mean((df1[c[0]][:n] - df2[c[1]][:n]) ** 2)
        amount += 1
    # One data df may be empty
    if amount == 0:
        return 0
    return float(c_sum / amount)


def align_unsmart(
    ts1: Timeseries,
    ts2: Timeseries,
    min_translation,
    max_translation,
    min_scale,
    max_scale,
):
    for translation in range(min_translation, max_translation):
        for scale in range(min_scale, max_scale):
            pass
    pass


def translate(ts: Timeseries, translation: float):
    ts.df[ts._time_column] = [x + int(translation) for x in ts.time_column()]
    if not translation.is_integer():
        lam = translation - int(translation)
        data_df = ts.data_df()
        for c in data_df:
            x = data_df[c].to_numpy()
            one_shift = pd.Series(x[1:]).to_numpy()
            one_shift = np.append(one_shift, x[-1])
            ts.df[c] = (1.0 - lam) * x + lam * one_shift


def transform(self, alignment: Alignment):
    norm_pipeline = Pipeline().push(normalization(-1.0, 1.0)).push(index_to_time)
    pipeline = Pipeline()
    new_n = int(self.ts2.df.shape[0] * alignment.scale)
    pipeline.push(interpolate_count(n=new_n)).push(index_to_time)
    ts_trans2 = pipeline.apply(self.ts2)
    translate(ts_trans2, alignment.translation)
    self.ts2 = norm_pipeline.apply(ts_trans2)
    self.ts1 = norm_pipeline.apply(self.ts1)


def align_smart(ts1: Timeseries, ts2: Timeseries):
    pass
