"""
Timeseries
----------
``Timeseries`` is the main representation of data in this library.

It contains of:
data:
    The `DataFrame` containing the relevant data.
indices:
    time_indices used for the time series.
"""

from typing import Optional
import pandas as pd
import tslib.utils as utils
import numpy as np


# The timeseries may also consist of following private fields
# self._time_column: [int | None] indicating the column of the indices in the df
# self._index_type: "integer" | "datetime" depening on the type of indices
class Timeseries:
    def __init__(self, data, time_column: Optional[str | int] = None):
        """
        Creates a new ``Timeseries`` object.
        The data has to be or be convertable to a DataFrame (:class:`pandas.DataFrame`).

        Parameters
        ----------
        data
            The DataFrame or a structure that can be converted as such.
        time_column
            The name or index of the column used for the `index_strategy`. The column should consist of integers or `DateTime`. If no `time_column` is set, the data will be indexed using the `DateFrame` index will be used directly.
        """
        if not isinstance(data, pd.DataFrame):
            utils.warn(
                "Data for `Timeseries` should be of type `DataFrame`. Trying to convert to `DataFrame`"
            )
            try:
                data = pd.DataFrame(data)
            except Exception as conversion_exception:
                raise conversion_exception
        if time_column:
            if isinstance(time_column, int):
                # The user has the responsibility to provide a valid index for the dataframe
                assert (
                    0 <= time_column < len(data.columns)
                ), "couldn't index `DataFrame` by `time_column`"
                index_column = time_column
            elif isinstance(time_column, str):
                # Can raise an exception in case the column name is not found.
                # It is the users responsibility to provide a valid column name from the database
                index_column = data.columns.get_loc(time_column)
            else:
                raise ValueError(
                    f"`time_columns` should be of type `int` or `str`, \
                                  but is of type {type(time_column)}"
                )
            self._time_column = index_column
            time_type = data.dtypes.iloc[index_column]
            # TODO getting rid of stringly typed specification

            # np.integer also covers python integers
            if np.issubdtype(time_type, np.integer):
                self._index_type = "integer"
            elif np.issubdtype(time_type, np.datetime64):
                self._index_type = "datetime"
            else:
                raise AttributeError(
                    "Invalid type of column referenced by `time_column`. Should be of type `int` or `datetime`"
                )
            # indices references the dataframe here which might lead to some weird sideeffects.
            # TODO consider copy or moving of the column
            self.indices = data.iloc[:, index_column]
        else:
            self.index_type = "integer"
            self.indices = [i for i in data.index]

        self.data = data
