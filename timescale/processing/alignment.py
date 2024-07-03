from timescale.processing.pipeline import (
    Pipeline,
    normalization,
    interpolate_count,
    index_to_time,
)
from timescale.timeseries import Timeseries
import pandas as pd
import numpy as np

from plotly.graph_objects import Figure
from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class Alignment:
    scale: float
    translation: float


def align(ts1, ts2, alignerclass):
    def inner(translation: float, scale: float):
        alignment = Alignment(scale, translation)
        aligner = alignerclass(ts1, ts2)
        aligner.transform(alignment)
        return aligner.alignment_score()

    return inner


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


@dataclass
class BaseAligner(ABC):
    ts1: Timeseries
    ts2: Timeseries

    def transform(self, alignment: Alignment):
        norm_pipeline = Pipeline().push(normalization(-1.0, 1.0)).push(index_to_time)
        pipeline = Pipeline()
        new_n = int(self.ts2.df.shape[0] * alignment.scale)
        pipeline.push(interpolate_count(n=new_n)).push(index_to_time)
        ts_trans2 = pipeline.apply(self.ts2)
        translate(ts_trans2, alignment.translation)
        self.ts2 = norm_pipeline.apply(ts_trans2)
        self.ts1 = norm_pipeline.apply(self.ts1)

    @abstractmethod
    def alignment_score(self) -> float:
        pass

    @abstractmethod
    # TODO remove from library side since it is not a feature of a library but should be implemented on the user side only
    def add_visualization(self, figure: Figure):
        pass


def calculate_eucl_distance(ts1: Timeseries, ts2: Timeseries):
    cols1 = [n + "_x" for n in ts1.data_df().columns]
    cols2 = [n + "_y" for n in ts2.data_df().columns]
    size = min(len(cols1), len(cols2))
    df = pd.merge(ts1.df, ts2.df, left_on=ts1._time_column, right_on=ts2._time_column)
    diff = np.abs(df[cols1[:size]].to_numpy() - df[cols2[:size]].to_numpy())
    df["result"] = 1.0 - diff
    return df


class EuclidianAligner(BaseAligner):
    def apply(self):
        if hasattr(self, "cache"):
            return self.cache
        else:
            self.cache = calculate_eucl_distance(self.ts1, self.ts2)
            return self.apply()

    def alignment_score(self):
        sums = self.apply()
        return np.sum(sums["result"])

    def add_visualization(self, figure: Figure):
        sums = self.apply()
        figure.add_bar(x=sums["timestamp"], y=sums["result"], name="sums")


def calculate_sum(ts1, ts2):
    df = pd.merge(ts1.df, ts2.df, left_on=ts1._time_column, right_on=ts2._time_column)
    df["result"] = df.loc[
        :,
        [
            a and b
            for a, b in zip(
                df.columns != ts1._time_column, df.columns != ts2._time_column
            )
        ],
    ].apply(lambda x: np.abs(np.sum(x)), axis=1)
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
        figure.add_bar(x=sums["timestamp"], y=sums["result"] - 1, name="sums")


def calculate_corr(ts1, ts2):
    df = pd.merge(ts1.df, ts2.df, left_on=ts1._time_column, right_on=ts2._time_column)

    df["result"] = df.loc[
        :,
        [
            a and b
            for a, b in zip(
                df.columns != ts1._time_column, df.columns != ts2._time_column
            )
        ],
    ].apply(np.prod, axis=1)
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
            x=correlations[self.ts1._time_column],
            y=correlations["result"],
            name="correlation",
        )
