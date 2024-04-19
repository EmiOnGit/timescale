from tslib.processing.pipeline import Pipeline, interpolate, index_to_time
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import numpy as np

from tslib.timeseries import Timeseries


class TsView:
    def __init__(self, ts1: Timeseries, ts2: Timeseries):
        self.ts1 = ts1
        self.ts2 = ts2
        self.scale = 1.0
        self.xoffset = 0

    def with_offset(self, offset):
        self.xoffset = offset

    def with_scale(self, scale):
        self.scale = scale

    def apply_ts2(self):
        pipeline = Pipeline()
        pipeline.push(interpolate(factor=self.scale)).push(index_to_time)

        return pipeline.apply(self.ts2)

    def calculate_corr(self, ts2):
        df = pd.merge(self.ts1.df, ts2.df, on="timestamp")

        df["corr"] = df.loc[:, df.columns != "timestamp"].apply(np.prod, axis=1)
        return df

    def draw(self):
        ts2 = self.apply_ts2()
        ts2.df[ts2._time_column] = [x + self.xoffset for x in ts2.df[ts2._time_column]]
        corr_df = self.calculate_corr(ts2)
        fig = go.Figure()
        fig.add_scatter(x=ts2.df["timestamp"], y=ts2.df["value-0"])
        fig.add_scatter(x=self.ts1.df["timestamp"], y=self.ts1.df["value-0"])
        fig.add_bar(x=corr_df["timestamp"], y=corr_df["corr"], name="correlation")
        return fig
