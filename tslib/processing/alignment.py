from dataclasses import dataclass
import scipy
from tslib.timeseries import Timeseries
import numpy as np
from bayes_opt import BayesianOptimization


@dataclass
class Alignment:
    translation: float
    scaling: float


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
