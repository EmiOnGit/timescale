import pandas as pd
import numpy as np
from tslib.processing.pipeline import (
    Pipeline,
    normalization,
    interpolate,
    index_to_time,
)
# from bayes_opt import BayesianOptimization


def aligner(ts1, ts2):
    def inner(scale, translation) -> float:
        return align_at(ts1, ts2, scale, translation)

    return inner


def align_at(ts1, ts2, scaling, translation) -> float:
    normalizer = Pipeline().push(normalization(-1.0, 1.0))
    transformer = Pipeline().push(interpolate(factor=scaling)).push(index_to_time)
    ts1_tr = normalizer.apply(ts1)
    ts2_tr = transformer.apply(ts2)
    ts2_tr.df[ts2_tr._time_column] = [
        x + int(translation) for x in ts2_tr.time_column()
    ]
    ts2_tr = normalizer.apply(ts2_tr)
    # print(ts1_tr.df)
    correlation = calculate_corr(ts1_tr, ts2_tr)
    return correlation


def calculate_corr(ts1, ts2) -> float:
    corr = calculate_corr_vec(ts1, ts2)
    return np.sum(corr["corr"])


def calculate_corr_vec(ts1, ts2):
    df = pd.merge(ts1.df, ts2.df, on="timestamp")

    df["corr"] = df.loc[:, df.columns != "timestamp"].apply(np.prod, axis=1)
    return df
