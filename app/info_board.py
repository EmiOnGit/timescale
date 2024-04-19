from app.ts_view import TsView
import numpy as np


class InfoBoard:
    def __init__(self):
        self.corr = 3

    def update(self, ts_view: TsView, scale, offset):
        ts_view.with_offset(offset)
        ts_view.with_scale(scale)
        ts2 = ts_view.apply_ts2()
        ts2.df[ts2._time_column] = [x + offset for x in ts2.df[ts2._time_column]]
        corr = ts_view.calculate_corr(ts2)

        self.corr = np.sum(corr["corr"])

    def draw(self):
        return f"Correlation: {self.corr}"
