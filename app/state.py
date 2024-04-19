from tslib.processing.pipeline import Pipeline
from tslib.processing.pipeline import (
    Pipeline,
    interpolate,
    index_to_time,
    normalization,
    mult,
    add,
)
import numpy as np
import pandas as pd


class State:
    def __init__(self, ts1, ts2):
        self.ts1 = ts1
        self.ts2 = ts2
        self.scale = 1.0
        self.offset = 0
        self.cached = ts2

        self.norm_pipeline = (
            Pipeline().push(normalization).push(mult(2.0)).push(add(-1.0))
        )
        self.ts1_normalized = self.norm_pipeline.apply(ts1)

    def transform_ts2(self, scale, offset):
        if scale == self.scale and offset == self.offset:
            return self.cached
        self.scale = scale
        self.offset = offset

        pipeline = Pipeline()
        pipeline.push(interpolate(factor=self.scale)).push(index_to_time)
        ts_trans = pipeline.apply(self.ts2)
        ts_trans.df[ts_trans._time_column] = [
            x + self.offset for x in ts_trans.time_column()
        ]
        self.cached = ts_trans
        return self.cached

    def get_corr(self):
        return calculate_corr(
            self.ts1_normalized, self.norm_pipeline.apply(self.cached)
        )


def calculate_corr(ts1, ts2):
    df = pd.merge(ts1.df, ts2.df, on="timestamp")

    df["corr"] = df.loc[:, df.columns != "timestamp"].apply(np.prod, axis=1)
    return df
