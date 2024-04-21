import pandas as pd
from tslib.timeseries import Timeseries
from tslib.processing.pipeline import *


def sample_ts():
    df = pd.DataFrame(data={"ticks": [1, 2, 3], "a": [3, 2, 4], "b": [-1.0, 4.0, 9.0]})
    df.set_index(pd.Index([2, 3, 4]), inplace=True)
    ts = Timeseries(df, time_column="ticks")
    return ts


def test_add():
    ts = sample_ts()
    pipeline = Pipeline()
    pipeline.push(add(1))
    ts2 = pipeline.apply(ts)
    assert all(ts2.df["a"] == [4, 3, 5])
    assert all(ts2.df["b"] == [0.0, 5.0, 10.0])
    assert all(ts2.time_column() == ts.time_column())
    pipeline.push(add(-1))
    ts3 = pipeline.apply(ts)
    assert ts.df.equals(ts3.df)
    pipeline = Pipeline()
    pipeline.push(add(1.5))
    ts4 = pipeline.apply(ts)
    assert all(ts4.df["a"] == [4.5, 3.5, 5.5])
    assert all(ts4.df["b"] == [0.5, 5.5, 10.5])


def test_normalize():
    ts = sample_ts()
    pipeline = Pipeline()
    pipeline.push(normalization())
    ts2 = pipeline.apply(ts)
    assert all(ts2.df["a"] == [0.5, 0.0, 1.0])
    assert all(ts2.df["b"] == [0.0, 0.5, 1.0])

    pipeline = Pipeline()
    pipeline.push(normalization(min=-1.0, max=1.0))
    ts2 = pipeline.apply(ts)
    print(ts2.df)
    assert all(ts2.df["a"] == [0.0, -1.0, 1.0])
    assert all(ts2.df["b"] == [-1.0, 0.0, 1.0])


def test_index_to_time():
    ts = sample_ts()
    pipeline = Pipeline()
    pipeline.push(index_to_time)
    ts2 = pipeline.apply(ts)
    assert all(ts2.time_column() == [2, 3, 4])
    assert ts.data_df().equals(ts2.data_df())


def test_interpolate():
    ts = sample_ts()
    pipeline = Pipeline()
    pipeline.push(interpolate(5.0 / 3.0))
    ts2 = pipeline.apply(ts)
    assert all(ts2.df["a"] == [3.0, 2.5, 2.0, 3.0, 4.0])
    assert all(ts2.df["b"] == [-1.0, 1.5, 4.0, 6.5, 9.0])
    assert all(ts2.time_column() == [1.0, 1.5, 2.0, 2.5, 3.0])


def test_cut_front():
    ts = sample_ts()
    pipeline = Pipeline()
    pipeline.push(cut_front(n=2, reindex=False))
    ts2 = pipeline.apply(ts)
    assert all(ts2.df["a"] == [4])
    assert all(ts2.df["b"] == [9.0])
    assert all(ts2.time_column() == [3])
    assert all(ts.df.index == [2, 3, 4])
    assert all(ts2.df.index == [4])
    pipeline = Pipeline()
    pipeline.push(cut_front(n=2, reindex=True))
    ts2 = pipeline.apply(ts)
    assert all(ts2.df["a"] == [4])
    assert all(ts2.df["b"] == [9.0])
    assert all(ts2.time_column() == [3])
    assert all(ts2.df.index == [2])
