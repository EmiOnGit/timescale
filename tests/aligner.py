import pandas as pd
from timescale.timeseries import Timeseries
from timescale.processing.pipeline import *
import timescale.processing.alignment as talign


def sample_ts():
    df = pd.DataFrame(data={"ticks": [1, 3, 5], "a": [3, 2, 4], "b": [-1.0, 4.0, 9.0]})
    df.set_index(pd.Index([2, 3, 4]), inplace=True)
    ts = Timeseries(df, time_column="ticks")
    return ts


def test_translate():
    default = sample_ts()
    ts = sample_ts()

    talign.translate(ts, 0)
    assert ts.df.equals(default.df)

    talign.translate(ts, 1)
    assert ts.data_df().equals(default.data_df())
    assert all(ts.time_column() == [2, 4, 6])

    talign.translate(ts, -1)
    assert ts.df.equals(default.df)

    talign.translate(ts, 1.5)
    assert all(ts.time_column() == [2, 4, 6])
    assert all(ts.df["a"] == [2.5, 3, 4])
    assert all(ts.df["b"] == [1.5, 6.5, 9.0])

    talign.translate(ts, -1.5)
    assert all(ts.time_column() == default.time_column())

    ts = sample_ts()
    talign.translate(ts, 0.1)
    assert all(ts.time_column() == default.time_column())
    assert all(np.abs(ts.df["a"] - [2.9, 2.2, 4.0]) < 0.001)
