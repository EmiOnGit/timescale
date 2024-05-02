from dataclasses import dataclass
from tslib.processing.pipeline import (
    Pipeline,
    interpolate,
    index_to_time,
    normalization,
)
import numpy as np
import pandas as pd

from tslib.timeseries import Timeseries


# class State:
#     def __init__(self, ts1, ts2):
#         self.ts1 = ts1
#         self.ts2 = ts2
#         self.scale = 1.0
#         self.offset = 0
#         self.cached = ts2

#         self.norm_pipeline = Pipeline().push(normalization(-1.0, 1.0))
#         self.ts1_normalized = self.norm_pipeline.apply(ts1)

#     def transform_ts2(self, scale, offset):
#         if scale == self.scale and offset == self.offset:
#             return self.cached
#         self.scale = scale
#         self.offset = offset

#         pipeline = Pipeline()
#         pipeline.push(interpolate(factor=self.scale)).push(index_to_time)
#         ts_trans = pipeline.apply(self.ts2)
#         ts_trans.df[ts_trans._time_column] = [
#             x + self.offset for x in ts_trans.time_column()
#         ]
#         self.cached = ts_trans
#         return self.cached


#     def get_corr(self):
#         return calculate_corr(
#             self.ts1_normalized, self.norm_pipeline.apply(self.cached)
#         )
@dataclass
class State:
    ts1: Timeseries
    ts2: Timeseries
    scale: float
    offset: int

    def transform_ts2(self):
        pipeline = Pipeline()
        pipeline.push(interpolate(factor=self.scale)).push(index_to_time)
        ts_trans2 = pipeline.apply(self.ts2)
        ts_trans2.df[ts_trans2._time_column] = [
            x + self.offset for x in ts_trans2.time_column()
        ]
        norm_pipeline = Pipeline().push(normalization(-1.0, 1.0))
        ts_trans2 = norm_pipeline.apply(ts_trans2)
        return ts_trans2

    def correlations(self):
        ts_trans2 = self.transform_ts2()
        norm_pipeline = Pipeline().push(normalization(-1.0, 1.0))
        ts1_normalized = norm_pipeline.apply(self.ts1)
        return calculate_corr(ts1_normalized, ts_trans2)


def calculate_corr(ts1, ts2):
    df = pd.merge(ts1.df, ts2.df, on="timestamp")

    df["corr"] = df.loc[:, df.columns != "timestamp"].apply(np.prod, axis=1)
    return df
