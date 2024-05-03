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
    offset: float


def align(ts1, ts2, alignerclass):
    def inner(translation: float, scale: float):
        alignment = Alignment(scale, offset=translation)
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
            x + int(alignment.offset) for x in ts_trans2.time_column()
        ]
        if not alignment.offset.is_integer():
            lam = alignment.offset - int(alignment.offset)
            data_df = ts_trans2.data_df()
            for c in data_df:
                x = data_df[c]
                one_shift = pd.Series(x[1:]).to_numpy()
                ts_trans2.df[c] = (1.0 - lam) * x + lam * np.append(
                    one_shift, x.values[-1]
                )

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
    # TODO remove from library side since it is not a feature of a library but should be implemented on the user side only
    def add_visualization(self, figure: Figure):
        pass


def calculate_sum(ts1, ts2):
    df = pd.merge(ts1.df, ts2.df, on="timestamp")
    df["result"] = df.loc[:, df.columns != "timestamp"].apply(
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
        return np.sum(sums["result"])

    def add_visualization(self, figure: Figure):
        sums = self.apply()
        figure.add_bar(x=sums["timestamp"], y=sums["result"], name="sums")


def calculate_corr(ts1, ts2):
    df = pd.merge(ts1.df, ts2.df, on="timestamp")

    df["result"] = df.loc[:, df.columns != "timestamp"].apply(np.prod, axis=1)
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
        return np.sum(correlations["result"])

    def add_visualization(self, figure: Figure):
        correlations = self.apply()
        figure.add_bar(
            x=correlations["timestamp"], y=correlations["result"], name="correlation"
        )
