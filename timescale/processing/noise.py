from numpy import random
from timescale.timeseries import Timeseries


def uniform_noise(amount=1.0):
    def _uniform_noise(ts: Timeseries):
        df = ts.data_df()
        for c in df:
            for x in range(len(df[c])):
                offset = random.rand() * amount - amount / 2.0
                value = df[c][x] + offset
                ts.df.loc[x, c] = value
        return ts

    return _uniform_noise
