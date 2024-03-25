from enum import Enum
import pandas as pd
import polars as pl
import tslib.utils as utils


class Timeseries:
    def __init__(self, data):
        if isinstance(data, pd.DataFrame):
            self.backend = Backend.Pandas
        elif isinstance(data, pl.DataFrame):
            self.backend = Backend.Polars
        else:
            utils.warn(
                "Data for `Timeseries` should be of type `pd.DataFrame` or `pl.DataFrame`. Trying to convert to `pd.DataFrame`"
            )
            try:
                data = pd.DataFrame(data)
                self.backend = Backend.Pandas
            except Exception as conversion_exception:
                raise conversion_exception
        self.data = data


class Backend(Enum):
    Pandas = 1
    Polars = 2
