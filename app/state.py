from dataclasses import dataclass
from typing import Tuple
from tslib.processing.pipeline import (
    Pipeline,
    interpolate,
    index_to_time,
    normalization,
)
from tslib.processing.alignment import SumAligner, CorrelationAligner, BaseAligner

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
    else:
        print(f"method name {method_name} couldn't be matched")
        return CorrelationAligner
