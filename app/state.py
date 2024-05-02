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
import json


@dataclass
class Settings:
    align_method: str
    points: int
    iterations: int


def alignment_or_default(input):
    if input is None:
        return Alignment(scale=1.0, offset=0)
    else:
        return Alignment(**json.loads(input))


@dataclass
class Alignment:
    scale: float
    offset: int


@dataclass
class ViewState:
    ts1: Timeseries
    ts2: Timeseries
    alignment: Alignment

    def transform_ts2(self):
        pipeline = Pipeline()
        pipeline.push(interpolate(factor=self.alignment.scale)).push(index_to_time)
        ts_trans2 = pipeline.apply(self.ts2)
        ts_trans2.df[ts_trans2._time_column] = [
            x + self.alignment.offset for x in ts_trans2.time_column()
        ]
        norm_pipeline = Pipeline().push(normalization(-1.0, 1.0))
        ts_trans2 = norm_pipeline.apply(ts_trans2)
        return ts_trans2

    def apply(self, align_method):
        ts_trans2 = self.transform_ts2()
        norm_pipeline = Pipeline().push(normalization(-1.0, 1.0))
        ts1_normalized = norm_pipeline.apply(self.ts1)
        if align_method == "correlation":
            return calculate_corr(ts1_normalized, ts_trans2)
        elif align_method == "function sum":
            return calculate_sum(ts1_normalized, ts_trans2)
        else:
            print(f"couldn't match align method: {align_method}")
            return pd.DataFrame({"corr": [0]})


def calculate_sum(ts1, ts2):
    df = pd.merge(ts1.df, ts2.df, on="timestamp")
    df["corr"] = df.loc[:, df.columns != "timestamp"].apply(
        lambda x: np.abs(np.sum(x)), axis=1
    )
    return df


def calculate_corr(ts1, ts2):
    df = pd.merge(ts1.df, ts2.df, on="timestamp")

    df["corr"] = df.loc[:, df.columns != "timestamp"].apply(np.prod, axis=1)
    return df
