from dataclasses import dataclass
from scipy import signal
from tslib.timeseries import Timeseries
import numpy as np
from bayes_opt import BayesianOptimization


@dataclass
class Alignment:
    translation: float
    scaling: float


class Aligner:
    def __init__(self, y1, y2):
        self.y1 = y1
        self.y2 = y2

    def interpolate_y2(self, xscale):
        # number of elements in y2
        num = int(len(self.y2) * xscale)
        x2_old = [x for x in range(len(self.y2))]
        # We need to substract one because we want x2new_max = x2_max
        x2 = np.linspace(0.0, len(self.y2) - 1, num=num)
        # by default [[1],[2],[3]] maps to [[1,2,3]] from 3 points (1) (2) (3)
        y2_columns = np.transpose(self.y2)
        y2_interp = [np.interp(x2, x2_old, column) for column in y2_columns]
        return y2_interp

    def score_alignment(self, xshift, xscale):
        num = int(len(self.y2) * xscale)
        mode = "full"

        y1_columns = np.transpose(self.y1)
        y2_interp = self.interpolate_y2(xscale)

        # normalize between -1,1
        y1_columns = [column / np.max(np.abs(column), axis=0) for column in y1_columns]
        y2_interp = [column / np.max(np.abs(column), axis=0) for column in y2_interp]

        # Correlation matrix
        # Calculates the correlation for by zipping y1 and y2 together and summing all dimmensions
        # TODO should be more inteligent, eg by using PCA beforehand on the ts with more dimensions
        correlation_matrix = [
            signal.correlate(y1col, y2col, mode=mode)
            for (y1col, y2col) in zip(y1_columns, y2_interp)
        ]
        correlations = [np.average(row) for row in np.transpose(correlation_matrix)]
        lags = signal.correlation_lags(len(self.y1), num, mode=mode)
        if len(lags) >= xshift:
            return -100.0
        return correlations[lags[xshift]]

    def align(self, method="correlation"):
        pass


def aligner(y1, y2):
    def blackbox(xshift, xscale):
        num = len(y2) * xscale

        x2 = np.linspace(0.0, len(y2), num=num)

        y2_interp = np.interp(x2, range(len(y2)), y2)
        cross_correlations = np.correlate(y1, y2_interp)
        if xshift >= len(cross_correlations):
            return -10000.0
        else:
            return cross_correlations[int(xshift)]

    return blackbox


def align(t1: Timeseries, t2: Timeseries) -> Alignment | None:
    pbounds = {"xshift": (0, 150), "xscale": (0.1, 10.0)}

    optimizer = BayesianOptimization(
        f=aligner(t1, t2),
        pbounds=pbounds,
        verbose=1,  # verbose = 1 prints only when a maximum is observed, verbose = 0 is silent
        random_state=1,
    )
    optimizer.maximize(
        init_points=20,
        n_iter=60,
    )
    pass
