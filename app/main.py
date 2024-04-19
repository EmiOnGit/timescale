#!/usr/bin/env python

import os, sys

pathname = os.path.dirname(sys.argv[0])
path = pathname + "/.."
# needed to import tslib when executing this file
sys.path.append(path)
import pandas as pd
from dash import Dash, html, dcc, callback, Output, Input
from tslib.processing.pipeline import *
from tslib.generator import generate
from ts_view import TsView
from info_board import InfoBoard

FACTOR = 2
OFFSET = 10
REAL_OFFSET = FACTOR * OFFSET


app = Dash(__name__)

app.layout = html.Div(
    [
        html.H1(children="TimeScale", style={"textAlign": "center"}),
        dcc.Graph(id="graph-content"),
        html.Div(id="correlation", style={"whiteSpace": "pre-line"}),
        html.Plaintext(children="scale", style={"textAlign": "center"}),
        dcc.Slider(
            id="slider_scale",
            min=0.0,
            max=5.0,
            step=0.1,
            value=1.0,
            marks={i: "{:.1f}".format(i) for i in np.linspace(0.01, 5.0, 21)},
        ),
        html.Plaintext(children="offset", style={"textAlign": "center"}),
        dcc.Slider(
            id="slider_offset",
            min=-100,
            max=100,
            step=1,
            value=0.0,
            marks={i: str(i) for i in range(-100, 100, 10)},
        ),
    ]
)


@callback(
    Output("correlation", "children"),
    Input("slider_scale", "value"),
    Input("slider_offset", "value"),
)
def update_correlation(scale, offset):
    info_board.update(ts_view, scale, offset)
    return info_board.draw()


@callback(
    Output("graph-content", "figure"),
    Input("slider_scale", "value"),
    Input("slider_offset", "value"),
)
def update_graph_scale(scale, offset):
    ts_view.with_scale(scale)
    ts_view.with_offset(offset)
    return ts_view.draw()


def init_data():
    ts = generate.generate_simple_with_noise(n=100, dimensions=1)
    ts2 = generate.generate_simple_with_noise(n=100, dimensions=1)
    pipeline = Pipeline()
    pipeline.push(interpolate_int(factor=FACTOR)).push(index_to_time)
    ts = pipeline.apply(ts)
    pipeline = Pipeline()
    # lazy
    pipeline.push(add(1))
    ts2 = pipeline.apply(ts2)
    ts2.df = pd.DataFrame(ts2.df[OFFSET:]).dropna(axis="rows")
    assert len(ts2.df["timestamp"]) == 90
    ts_view = TsView(ts, ts2)
    info_board = InfoBoard()
    return ts_view, info_board


if __name__ == "__main__":
    ts_view, info_board = init_data()
    app.run(debug=True)
