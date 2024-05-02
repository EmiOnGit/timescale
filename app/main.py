#!/usr/bin/env python
import plotly.graph_objects as go
import pyarrow as pa
import pyarrow.parquet as pq
import os, sys

from app.state import State


pathname = os.path.dirname(sys.argv[0])
path = pathname + "/.."
# needed to import tslib when executing this file
sys.path.append(path)
import pandas as pd
from dash import Dash, html, dcc, callback, Output, Input
import dash_bootstrap_components as dbc
from tslib.processing.pipeline import *
from tslib.generator import generate
import tslib.io as tio
from ts_view import TsView
from info_board import InfoBoard
import numpy as np

FACTOR = 2
OFFSET = 10
REAL_OFFSET = FACTOR * OFFSET


app = Dash(name="timescale", external_stylesheets=[dbc.themes.BOOTSTRAP])


timeseries_block = [
    dbc.Col(
        [
            html.H1(f"ts{i}", style={"textAlign": "center"}),
            html.Div(id=f"info_n{i}", children="n="),
            dcc.Upload(
                id=f"info_upload{i}",
                children=html.Div([f"Upload for ts{i}"]),
                style={
                    "width": "50%",
                    "lineHeight": "60px",
                    "borderWidth": "1px",
                    "borderStyle": "dashed",
                    "borderRadius": "5px",
                    "textAlign": "center",
                    "margin": "10px",
                },
                multiple=False,
            ),
        ]
    )
    for i in range(2)
]
app.layout = html.Div(
    [
        html.H1(children="TimeScale", style={"textAlign": "center"}),
        dbc.Row([timeseries_block[0], timeseries_block[1]]),
        html.Div(id="correlation", style={"whiteSpace": "pre-line"}),
        dcc.Graph(id="graph-content"),
        html.Plaintext(children="scale", style={"textAlign": "center"}),
        dcc.Slider(
            id="slider_scale",
            min=0.0,
            max=5.0,
            step=0.02,
            value=1.0,
            marks={i: "{:.1f}".format(i) for i in np.linspace(0.01, 5.0, 21)},
        ),
        html.Div(
            [
                html.Plaintext(children="offset", style={"textAlign": "center"}),
                dcc.Slider(
                    id="slider_offset",
                    min=-100,
                    max=100,
                    step=1,
                    value=0.0,
                    marks={i: str(i) for i in range(-100, 100, 10)},
                ),
            ],
        ),
        dcc.Store(id="ts1store"),
        dcc.Store(id="ts2store"),
    ]
)

import base64


@callback(
    Output("ts1store", "data"),
    Input("info_upload0", "contents"),
)
def register_upload(content):
    if content is None:
        ts = default_ts1()
        return tio.to_json(ts)
    content = content.split(",")[1]
    decoded = base64.b64decode(content)

    reader = pa.BufferReader(decoded)
    table = pq.read_table(reader)
    ts = tio.ts_from_arrow_table(table)
    return tio.to_json(ts)


@callback(
    Output("ts2store", "data"),
    Input("info_upload1", "contents"),
)
def register_upload2(content):
    if content is None:
        ts = default_ts2()
        return tio.to_json(ts)
    content = content.split(",")[1]
    decoded = base64.b64decode(content)

    reader = pa.BufferReader(decoded)
    table = pq.read_table(reader)
    ts = tio.ts_from_arrow_table(table)
    return tio.to_json(ts)


@callback(Output("info_n0", "children"), Input("ts1store", "data"))
def on_ts_change(ts1json):
    ts1 = tio.from_json(ts1json)
    return f"n: {len(ts1.df)}"


@callback(Output("info_n1", "children"), Input("ts2store", "data"))
def on_ts2_change(ts2json):
    ts2 = tio.from_json(ts2json)
    return f"n: {len(ts2.df)}"


@callback(
    Output("correlation", "children"),
    Input("ts1store", "data"),
    Input("ts2store", "data"),
    Input("slider_scale", "value"),
    Input("slider_offset", "value"),
)
def update_correlation(ts1, ts2, scale, offset):
    ts1, ts2 = tio.from_json(ts1), tio.from_json(ts2)
    state = State(ts1, ts2, scale, offset)
    correlations = state.correlations()
    sum = np.sum(correlations["corr"])
    return f"Correlation: {sum}"


@callback(
    Output("graph-content", "figure"),
    Input("ts1store", "data"),
    Input("ts2store", "data"),
    Input("slider_scale", "value"),
    Input("slider_offset", "value"),
)
def update_graph(ts1, ts2, scale, offset):
    ts1, ts2 = tio.from_json(ts1), tio.from_json(ts2)
    state = State(ts1, ts2, scale, offset)
    ts2_trans = state.transform_ts2()
    correlations = state.correlations()
    fig = go.Figure()
    fig.add_scatter(x=ts2_trans.df["timestamp"], y=ts2_trans.df["value-0"])
    fig.add_scatter(x=ts1.df["timestamp"], y=ts1.df["value-0"])
    fig.add_bar(x=correlations["timestamp"], y=correlations["corr"], name="correlation")
    return fig


def default_ts1():
    ts = generate.generate_simple_with_noise(n=100, dimensions=1)
    pipeline = Pipeline()
    pipeline.push(interpolate(factor=FACTOR)).push(index_to_time)
    ts = pipeline.apply(ts)
    return ts


def default_ts2():
    ts2 = generate.generate_simple_with_noise(n=100, dimensions=1)
    pipeline = Pipeline()
    pipeline.push(add(1))
    ts2 = pipeline.apply(ts2)
    ts2.df = pd.DataFrame(ts2.df[OFFSET:]).dropna(axis="rows")
    return ts2


def init_data():
    ts = generate.generate_simple_with_noise(n=100, dimensions=1)
    ts2 = generate.generate_simple_with_noise(n=100, dimensions=1)
    pipeline = Pipeline()
    pipeline.push(interpolate(factor=FACTOR)).push(index_to_time)
    ts = pipeline.apply(ts)
    pipeline = Pipeline()
    # lazy
    pipeline.push(add(1))
    ts2 = pipeline.apply(ts2)
    ts2.df = pd.DataFrame(ts2.df[OFFSET:]).dropna(axis="rows")
    ts_view = TsView()
    info_board = InfoBoard()
    return ts_view, info_board


if __name__ == "__main__":
    ts_view, info_board = init_data()
    app.run(debug=True)
