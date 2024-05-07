from dataclasses import dataclass
from typing import Tuple

from bayes_opt import ScreenLogger
from tslib.processing.pipeline import (
    Pipeline,
    interpolate,
    index_to_time,
    normalization,
)
from tslib.processing.alignment import SumAligner, CorrelationAligner, EuclidianAligner

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


def estimate_scale_radius(ts1: Timeseries, ts2: Timeseries, l=2.0):
    f = len(ts1.df) / len(ts2.df)
    return f / l, f * l


def estimate_bounds(ts1, ts2, scale_freedom=1.6, percent_in_bounds=0.9):
    assert scale_freedom >= 1
    assert percent_in_bounds < 1.0
    assert percent_in_bounds > 0.0
    lower_scale, upper_scale = estimate_scale_radius(ts1, ts2, scale_freedom)
    # max_overflow = upper * len(ts2.df) - len(ts1.df)
    lower_offset = -lower_scale * len(ts2.df) * (1.0 - percent_in_bounds)
    upper_offset = max(len(ts1.df), len(ts2.df)) * percent_in_bounds
    return lower_offset, upper_offset, lower_scale, upper_scale


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

    def transform(self) -> Tuple[Timeseries, Timeseries]:
        ts_trans2 = self.transform_ts2()
        norm_pipeline = Pipeline().push(normalization(-1.0, 1.0))
        ts1_normalized = norm_pipeline.apply(self.ts1)
        return ts1_normalized, ts_trans2


def method_to_aligner(method_name: str):
    method_name = method_name.lower()
    if method_name == "correlation":
        return CorrelationAligner
    elif method_name == "function sum":
        return SumAligner
    elif method_name == "eucl":
        return EuclidianAligner
    else:
        print(f"method name {method_name} couldn't be matched")
        return CorrelationAligner


class ProgressLogger(ScreenLogger):
    def __init__(self, max, f):
        self.current = 0
        self.f = f
        self.max = max
        super().__init__(verbose=1)

    def update(self, event, instance):
        if self.current % 8 == 0:
            cur, max = str(self.current), str(self.max)
            self.f((cur, max))
        self.current += 1
        super().update(event, instance)
