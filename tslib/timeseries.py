"""
Timeseries
----------
``Timeseries`` is the main representation of data in this library.
"""

import pandas as pd
import tslib.utils as utils
import numpy as np

DEFAULT_TIME_COLUMN = "time"


# The timeseries may also consist of following private fields
# self._time_column: [str | None] indicating the column of the indices in the df
class Timeseries:
    def __init__(self, df, time_column: str | int = DEFAULT_TIME_COLUMN):
        """
        Creates a new ``Timeseries`` object.
        The df has to be or be convertable to a DataFrame (:class:`pandas.DataFrame`).

        Parameters
        ----------
        df
            The `DataFrame` or a structure that can be converted as such.
        time_column
            The name of the column used as "time" column.
            The column itself should be a subclass of :class:`np.integer`, :class:`float` or :class: `np.datetime64`.
        """
        if not isinstance(df, pd.DataFrame):
            utils.warn(
                "Data for `Timeseries` should be of type `DataFrame`. Trying to convert to `DataFrame`."
            )
            try:
                df = pd.DataFrame(df)
            except Exception as conversion_exception:
                raise conversion_exception
        if time_column:
            if isinstance(time_column, int):
                # The user has the responsibility to provide a valid index for the dataframe
                assert (
                    0 <= time_column < len(df.columns)
                ), "Couldn't index `DataFrame` by `time_column`"
                time_column = str(df.columns[time_column])
            elif isinstance(time_column, str):
                if not time_column in df.columns:
                    raise IndexError(
                        f"Column '{time_column}' not found in `DataFrame`."
                    )
            else:
                raise ValueError(
                    f"Argument `time_column` should be of type `int` or `str`, \
                                  but is of type {type(time_column)}."
                )
            self._time_column = time_column
        self.df = df

    def validate(self):
        """
        Valides the `Timeseries`. If the `Timeseries` is not valid, a exception is raised accordingly.
        A valid `Timeseries` does contain a valid "time" column in the `DataFrame`.
        A valid time column has elements of a sub class from :class:`np.integer`, :class:`float` or :class: `np.datetime64`
        and is strictly sorted in ascending order.
        Raises an corresponding exception if the `Timeseries` is no valid.

        Exceptions
        ----------
        IndexError
            `self._time_column` is not found as a column in the `DataFrame`
        AttributeError
            if the time column doesn't contain a valid type
        AssertionError
            if the time column is not strictly ordered

        Read the error messages for more information
        """
        # Check if time_column does exist in data frame.
        # It may be removed after creation
        if self._time_column not in self.df.columns:
            raise IndexError(
                f"Validation failed. Column '{self._time_column}' not found in `self.df`.",
            )
        time_type = self.df.dtypes[self._time_column]

        # Check if the time column contains a valid datatype.
        if (
            not np.issubdtype(time_type, np.integer)
            and not np.issubdtype(time_type, np.datetime64)
            and not np.issubdtype(time_type, float)
        ):
            raise AttributeError(
                "Invalid type of column referenced by `time_column`. Should be of type :class:`np.integer`, :class:`float` or :class:`np.datetime64`."
            )
        time_values = self.df[self._time_column]
        if any(
            time_values[i] >= time_values[i + 1] for i in range(len(time_values) - 1)
        ):
            raise AssertionError("time column has to be strictly ordered.")

    def is_valid(self, debug=False) -> bool:
        """
        Returns if the `Timeseries` is valid.
        See :func:`Timeseries.validate()` for more information

        Parameters
        ----------
        debug
            prints the error in case the `Timeseries` is not valid, if `true`.
        """
        try:
            self.validate()
            return True
        except Exception as e:
            if debug:
                utils.warn(e)
            return False
