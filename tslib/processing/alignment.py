import pandas as pd
import numpy as np
from tslib.processing.pipeline import (
    Pipeline,
    normalization,
    interpolate,
    index_to_time,
)

# from bayes_opt import BayesianOptimization
from plotly.graph_objects import Figure
from abc import ABC, abstractmethod
from tslib.timeseries import Timeseries
from dataclasses import dataclass


@dataclass
class Alignment:
    scale: float
    offset: int


def align(ts1, ts2, alignerclass):
    def inner(translation: int, scale: float):
        alignment = Alignment(scale, offset=int(translation))
        aligner = alignerclass(ts1, ts2)
        aligner.transform(alignment)
        return aligner.alignment_score()

    return inner


@dataclass
class BaseAligner(ABC):
    ts1: Timeseries
    ts2: Timeseries

    def transform(self, alignment: Alignment):
        pipeline = Pipeline()
        pipeline.push(interpolate(factor=alignment.scale)).push(index_to_time)
        ts_trans2 = pipeline.apply(self.ts2)
        ts_trans2.df[ts_trans2._time_column] = [
            x + alignment.offset for x in ts_trans2.time_column()
        ]
        norm_pipeline = Pipeline().push(normalization(-1.0, 1.0))
        ts_trans2 = norm_pipeline.apply(ts_trans2)
        norm_pipeline = Pipeline().push(normalization(-1.0, 1.0))
        ts1_normalized = norm_pipeline.apply(self.ts1)
        self.ts1 = ts1_normalized
        self.ts2 = ts_trans2

    @abstractmethod
    def alignment_score(self) -> float:
        pass

    @abstractmethod
    def add_visualization(self, figure: Figure):
        pass


def calculate_sum(ts1, ts2):
    df = pd.merge(ts1.df, ts2.df, on="timestamp")
    df["corr"] = df.loc[:, df.columns != "timestamp"].apply(
        lambda x: np.abs(np.sum(x)), axis=1
    )
    return df


class SumAligner(BaseAligner):
    def apply(self):
        if hasattr(self, "cache"):
            return self.cache
        else:
            self.cache = calculate_sum(self.ts1, self.ts2)
            return self.apply()

    def alignment_score(self):
        sums = self.apply()
        return np.sum(sums["corr"])

    def add_visualization(self, figure: Figure):
        sums = self.apply()
        figure.add_bar(x=sums["timestamp"], y=sums["corr"], name="sums")


def calculate_corr(ts1, ts2):
    df = pd.merge(ts1.df, ts2.df, on="timestamp")

    df["corr"] = df.loc[:, df.columns != "timestamp"].apply(np.prod, axis=1)
    return df


class CorrelationAligner(BaseAligner):
    def apply(self):
        if hasattr(self, "cache"):
            return self.cache
        else:
            self.cache = calculate_corr(self.ts1, self.ts2)
            return self.apply()

    def alignment_score(self):
        correlations = self.apply()
        return np.sum(correlations["corr"])

    def add_visualization(self, figure: Figure):
        correlations = self.apply()
        figure.add_bar(
            x=correlations["timestamp"], y=correlations["corr"], name="correlation"
        )


# def aligner(ts1, ts2):
#     def inner(scale, translation) -> float:
#         return align_at(ts1, ts2, scale, translation)

#     return inner


# def align_at(ts1, ts2, scaling, translation) -> float:
#     normalizer = Pipeline().push(normalization(-1.0, 1.0))
#     transformer = Pipeline().push(interpolate(factor=scaling)).push(index_to_time)
#     ts1_tr = normalizer.apply(ts1)
#     ts2_tr = transformer.apply(ts2)
#     ts2_tr.df[ts2_tr._time_column] = [
#         x + int(translation) for x in ts2_tr.time_column()
#     ]
#     ts2_tr = normalizer.apply(ts2_tr)
#     # print(ts1_tr.df)
#     correlation = calculate_corr(ts1_tr, ts2_tr)
#     return correlation


# def calculate_corr(ts1, ts2) -> float:
#     corr = calculate_corr_vec(ts1, ts2)
#     return np.sum(corr["corr"])


# def calculate_corr_vec(ts1, ts2):
#     df = pd.merge(ts1.df, ts2.df, on="timestamp")

#     df["corr"] = df.loc[:, df.columns != "timestamp"].apply(np.prod, axis=1)
#     return df
