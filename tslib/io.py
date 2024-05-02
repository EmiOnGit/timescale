from tslib.timeseries import Timeseries
import pyarrow as pa
import os
import pandas as pd


def write_as_parquet(ts: Timeseries, filepath: str | os.PathLike):
    """Writes a `Timeseries` object to a parquet file.
    This function requires pyarrow to run.

    Parameters
    ----------
    ts
        The timeseries which should be written to a file
    filepath
        The path of the parquet file
    """
    import pyarrow.parquet as pq

    df = ts.df
    table = pa.Table.from_pandas(df)
    existing_metadata = table.schema.metadata
    timescale_metadata = {b"time_column": ts._time_column.encode()}
    merged_metadata = {**existing_metadata, **timescale_metadata}
    table = table.replace_schema_metadata(merged_metadata)

    pq.write_table(table, filepath)


def ts_from_arrow_table(table, time_column=None) -> Timeseries:
    df = table.to_pandas()
    if not time_column is None:
        return Timeseries(df=df, time_column=time_column)
    existing_metadata = table.schema.metadata
    encoded_tc = existing_metadata.get(b"time_column")
    if encoded_tc is None:
        return Timeseries(df=df)
    else:
        return Timeseries(df=df, time_column=encoded_tc.decode("UTF-8"))


def read_from_parquet_file(
    filepath: str | os.PathLike, time_column: str | int | None = None
) -> Timeseries:
    """Reads a `Timeseries` object from a file.
    This function requires pyarrow to run

    Parameters
    ----------
    filepath
        The path of the parquet file
    time_column
        The name of the column, which should be used for the `Timeseries`.
        If `timescale.io.write_as_parquet` was used to write the file, the `time_column` was already written to the metadata of the file.
        Furthermore a column 'time' is searched, if no metadata entry was found nor `time_column` was provided.
    """
    import pyarrow.parquet as pq

    table = pq.read_table(filepath)
    return ts_from_arrow_table(table, time_column)


def to_json(ts: Timeseries) -> str:
    import json

    ts_repr = ts.__dict__.copy()
    ts_repr["df"] = ts.df.to_json(orient="records")
    return json.dumps(ts_repr)


def from_json(input: str) -> Timeseries:
    import json

    ts_repr = json.loads(input)
    df_repr = json.loads(ts_repr["df"])
    df = pd.json_normalize(df_repr)
    return Timeseries(df, ts_repr["_time_column"])
