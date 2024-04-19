import numpy as np


class InfoBoard:
    def __init__(self):
        self.corr = 3

    def update(self, state, scale, offset):
        state.transform_ts2(scale, offset)
        corr = state.get_corr()
        self.corr = np.sum(corr["corr"])

    def draw(self):
        return f"Correlation: {self.corr}"
