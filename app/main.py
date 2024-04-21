#!/usr/bin/env python
import pyarrow as pa
import pyarrow.parquet as pq
import tslib.io as tio
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
    ]
)

import base64


@callback(
    Output("info_n0", "children"),
    Input("info_upload0", "contents"),
)
def register_upload(content):
    if content is not None:
        content = content.split(",")[1]
        decoded = base64.b64decode(content)

        reader = pa.BufferReader(decoded)
        table = pq.read_table(reader)
        ts = tio.ts_from_arrow_table(table)
        state.ts1_normalized = state.norm_pipeline.apply(ts)
        state.ts1 = ts

    return f"n: {len(state.ts1.df)}"


@callback(
    Output("info_n1", "children"),
    Input("info_upload1", "contents"),
)
def register_upload2(content):
    if content is not None:
        content = content.split(",")[1]
        decoded = base64.b64decode(content)

        reader = pa.BufferReader(decoded)
        table = pq.read_table(reader)
        ts = tio.ts_from_arrow_table(table)
        state.ts2 = ts

    return f"n: {len(state.ts2.df)}"


@callback(
    Output("correlation", "children"),
    Input("slider_scale", "value"),
    Input("slider_offset", "value"),
)
def update_correlation(scale, offset):
    info_board.update(state, scale, offset)
    return info_board.draw()


@callback(
    Output("graph-content", "figure"),
    Input("slider_scale", "value"),
    Input("slider_offset", "value"),
    Input("info_n0", "children"),
    Input("info_n1", "children"),
)
def update_graph(scale, offset, _s, _i):
    del _s, _i
    state.transform_ts2(scale, offset)
    return ts_view.draw(state.ts1, state)


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
    state = State(ts, ts2)
    ts_view = TsView()
    info_board = InfoBoard()
    return ts_view, info_board, state


if __name__ == "__main__":
    ts_view, info_board, state = init_data()
    app.run(debug=True)
