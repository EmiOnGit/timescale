#!/usr/bin/env python
from bayes_opt import BayesianOptimization
import plotly.graph_objects as go
import pyarrow as pa
import pyarrow.parquet as pq
from dash import Dash, State, html, dcc, callback, Output, Input
import json
import dash_bootstrap_components as dbc

import pandas as pd
import numpy as np
from app.state import Alignment, Settings, ViewState, alignment_or_default
import base64

import os
import sys

pathname = os.path.dirname(sys.argv[0])
path = pathname + "/.."
# needed to import tslib when executing this file
sys.path.append(path)
from tslib.processing.pipeline import Pipeline, add, interpolate, index_to_time
from tslib.generator import generate
import tslib.processing.alignment as tsalign
import tslib.io as tio

FACTOR = 2
OFFSET = 10
REAL_OFFSET = FACTOR * OFFSET


app = Dash(name="timescale", external_stylesheets=[dbc.themes.BOOTSTRAP])


timeseries_block = [
    dbc.Col(
        [
            html.H1(f"ts{i}", style={"textAlign": "center"}),
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
                    "margin-right": "70px",
                    "margin-left": "70px",
                },
                multiple=False,
            ),
            html.Div(id=f"info_n{i}", children="n="),
        ],
        style={
            "display": "flex",
            "flex-direction": "column",
            "align-items": "center",
            "justify-content": "center",
        },
    )
    for i in range(1, 3)
]
settings_block = dbc.Col(
    [
        html.H1("Settings", style={"textAlign": "center"}),
        dcc.Dropdown(
            ["correlation", "function sum"],
            "correlation",
            id="method_dropdown",
            style={
                "width": "220px",
                "textAlign": "center",
                "margin-bottom": "10px",
            },
        ),
        html.Div(
            [
                html.Div("points"),
                dcc.Slider(
                    id="slider_points",
                    min=1,
                    max=500,
                    step=1,
                    value=100,
                    marks={i: "{}".format(i) for i in range(0, 501, 100)},
                    # style={"width": "200px"},
                ),
                html.Div("iterations"),
                dcc.Slider(
                    id="slider_iterations",
                    min=1,
                    max=80,
                    step=1,
                    value=16,
                    marks={i: "{}".format(i) for i in range(0, 81, 10)},
                ),
            ],
            style={"width": "300px"},
        ),
        html.Button("Align", id="calculate_align"),
    ],
    style={
        "display": "flex",
        "flex-direction": "column",
        "align-items": "center",
        "justify-content": "center",
    },
)
app.layout = html.Div(
    [
        html.H1(children="TimeScale", style={"textAlign": "center"}),
        dbc.Row([timeseries_block[0], timeseries_block[1], settings_block]),
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
        dcc.Store(id="alignment"),
        dcc.Store(id="settings"),
    ]
)


@callback(
    Output("alignment", "data", allow_duplicate=True),
    Input("calculate_align", "n_clicks"),
    State("ts1store", "data"),
    State("ts2store", "data"),
    State("settings", "data"),
    prevent_initial_call=True,
)
def align(clicks, ts1json, ts2json, settings):
    del clicks
    ts1 = tio.from_json(ts1json)
    ts2 = tio.from_json(ts2json)
    settings = Settings(**json.loads(settings))
    pbounds = {"translation": (-200, 200), "scale": (0.2, 5.0)}
    optimizer = BayesianOptimization(
        f=tsalign.aligner(ts1, ts2),
        pbounds=pbounds,
        verbose=1,  # verbose = 1 prints only when a maximum is observed, verbose = 0 is silent
    )
    optimizer.maximize(
        init_points=settings.points,
        n_iter=settings.iterations,
    )
    print("finished alignment search")
    best = optimizer.max["params"]
    if best is None:
        return alignment_or_default(None)
    else:
        scale = best["scale"]
        offset = int(best["translation"])
        return json.dumps(Alignment(scale, offset).__dict__)


@callback(
    Output("settings", "data"),
    Input("slider_points", "value"),
    Input("slider_iterations", "value"),
    Input("method_dropdown", "value"),
)
def register_settings(points, iterations, align_method):
    return json.dumps(
        Settings(align_method, points=points, iterations=iterations).__dict__
    )


@callback(
    Output("ts1store", "data"),
    Input("info_upload1", "contents"),
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
    Input("info_upload2", "contents"),
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


@callback(Output("info_n1", "children"), Input("ts1store", "data"))
def info_ts1(ts1json):
    ts1 = tio.from_json(ts1json)
    return f"n: {len(ts1.df)}"


@callback(Output("info_n2", "children"), Input("ts2store", "data"))
def info_ts2(ts2json):
    ts2 = tio.from_json(ts2json)
    return f"n: {len(ts2.df)}"


@callback(
    Output("alignment", "data", allow_duplicate=True),
    Input("slider_scale", "value"),
    Input("slider_offset", "value"),
    prevent_initial_call=True,
)
def alignment_slider(scale, offset):
    return json.dumps(Alignment(scale, offset).__dict__)


@callback(
    Output("correlation", "children"),
    Input("ts1store", "data"),
    Input("ts2store", "data"),
    Input("alignment", "data"),
    Input("settings", "data"),
)
def update_correlation(ts1, ts2, alignment, settings):
    ts1, ts2 = tio.from_json(ts1), tio.from_json(ts2)
    settings = Settings(**json.loads(settings))
    alignment = alignment_or_default(alignment)
    state = ViewState(ts1, ts2, alignment)
    correlations = state.apply(settings.align_method)
    sum = np.sum(correlations["corr"])
    return f"Correlation: {sum}"


@callback(
    Output("graph-content", "figure"),
    Input("ts1store", "data"),
    Input("ts2store", "data"),
    Input("alignment", "data"),
    Input("settings", "data"),
)
def update_graph(ts1, ts2, alignment, settings):
    ts1, ts2 = tio.from_json(ts1), tio.from_json(ts2)
    alignment = alignment_or_default(alignment)
    settings = Settings(**json.loads(settings))
    state = ViewState(ts1, ts2, alignment)
    ts2_trans = state.transform_ts2()
    correlations = state.apply(settings.align_method)
    fig = go.Figure()
    fig.layout.__setattr__("uirevision", "const")
    fig.add_scatter(x=ts2_trans.df["timestamp"], y=ts2_trans.df["value-0"], name="ts2")
    # if 'uirevision' stays as it was before updating the figure, the zoom/ui will not reset. 'const' as value is arbitrary
    fig.add_scatter(x=ts1.df["timestamp"], y=ts1.df["value-0"], name="ts1")

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
    pipeline.push(add(4))
    ts2 = pipeline.apply(ts2)
    ts2.df = pd.DataFrame(ts2.df[OFFSET:]).dropna(axis="rows")
    return ts2


if __name__ == "__main__":
    app.run(debug=True)
