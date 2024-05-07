#!/usr/bin/env python
from bayes_opt import BayesianOptimization
from bayes_opt.event import Events
import plotly.graph_objects as go
import pyarrow as pa
import pyarrow.parquet as pq
from dash import Dash, State, callback, Output, Input, ctx, DiskcacheManager
import diskcache
import json
import dash_bootstrap_components as dbc

import pandas as pd
from app.state import (
    Alignment,
    ProgressLogger,
    Settings,
    ViewState,
    alignment_or_default,
    estimate_bounds,
    method_to_aligner,
)
import app.layout as layout
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

FACTOR = 3
OFFSET = 20

REAL_OFFSET = FACTOR * OFFSET

cache = diskcache.Cache("./cache")
background_callback_manager = DiskcacheManager(cache)

app = Dash(name="timescale", external_stylesheets=[dbc.themes.BOOTSTRAP])
app.layout = layout.layout()


@callback(
    Output("alignment", "data", allow_duplicate=True),
    Input("calculate_align", "n_clicks"),
    State("ts1store", "data"),
    State("ts2store", "data"),
    State("settings", "data"),
    background=True,
    manager=background_callback_manager,
    prevent_initial_call=True,
    running=[
        (
            Output("align_progress_box", "style"),
            {"visibility": "visible"},
            {"visibility": "hidden"},
        )
    ],
    cancel=Input("cancel_align_button", "n_clicks"),
    progress=[Output("progress_bar", "value"), Output("progress_bar", "max")],
)
def align(set_progress, clicks, ts1json, ts2json, settings):
    del clicks
    ts1 = tio.from_json(ts1json)
    ts2 = tio.from_json(ts2json)

    settings = Settings(**json.loads(settings))
    total = settings.points + settings.iterations

    lower_offset, upper_offset, lower_scale, upper_scale = estimate_bounds(ts1, ts2)
    pbounds = {
        "translation": (lower_offset, upper_offset),
        "scale": (lower_scale, upper_scale),
    }
    aligner_class = method_to_aligner(settings.align_method)

    logger = ProgressLogger(total, set_progress)
    optimizer = BayesianOptimization(
        f=tsalign.align(ts1, ts2, aligner_class),
        pbounds=pbounds,
    )
    optimizer.subscribe(Events.OPTIMIZATION_STEP, logger)

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


def ts_factory(i):
    def register_upload(filename, content):
        if content is None:
            if i == 1:
                ts = default_ts1()
            else:
                ts = default_ts2()
            return tio.to_json(ts), "default dataset"
        content = content.split(",")[1]
        decoded = base64.b64decode(content)

        reader = pa.BufferReader(decoded)
        table = pq.read_table(reader)
        ts = tio.ts_from_arrow_table(table)
        # TODO!!! time column has to be of the same time between ts1 and ts2. Not sure if we want to push index to time column
        # pipeline = Pipeline()
        # pipeline.push(index_to_time)
        # ts = pipeline.apply(ts)
        return tio.to_json(ts), f"file: {filename}"

    return register_upload


for i in range(1, 3):
    app.callback(
        Output(f"ts{i}store", "data"),
        Output(f"info_filepath{i}", "children"),
        State(f"info_upload{i}", "filename"),
        Input(f"info_upload{i}", "contents"),
    )(ts_factory(i))

    def info_ts(ts_json):
        ts = tio.from_json(ts_json)
        return f"n: {len(ts.df)}"

    app.callback(Output(f"info_n{i}", "children"), Input(f"ts{i}store", "data"))(
        info_ts
    )


@callback(
    Output("alignment", "data", allow_duplicate=True),
    Output("input_scale", "value"),
    Output("input_offset", "value"),
    Input("alignment", "data"),
    Input("input_scale", "value"),
    Input("input_offset", "value"),
    prevent_initial_call=True,
)
def alignment_slider(alignment_data, scale, offset):
    trigger_id = ctx.triggered[0]["prop_id"].split(".")[0]
    if trigger_id == "alignment":
        alignment = alignment_or_default(alignment_data)
    else:
        alignment = Alignment(scale, offset)
    return json.dumps(alignment.__dict__), alignment.scale, alignment.offset


@callback(
    Output("align_score", "children"),
    Output("graph-content", "figure"),
    Input("ts1store", "data"),
    Input("ts2store", "data"),
    Input("alignment", "data"),
    Input("settings", "data"),
)
def update_graph(ts1, ts2, alignment, settings):
    # deserialize from data storage
    ts1, ts2 = tio.from_json(ts1), tio.from_json(ts2)
    alignment = alignment_or_default(alignment)
    settings = Settings(**json.loads(settings))
    state = ViewState(ts1, ts2, alignment)

    ts1_trans, ts2_trans = state.transform()
    aligner = method_to_aligner(settings.align_method)
    aligner = aligner(ts1_trans, ts2_trans)

    # draw
    fig = go.Figure()
    fig.layout.__setattr__("uirevision", "const")

    fig.add_scatter(
        x=ts2_trans.time_column(),
        y=ts2_trans.data_df().iloc[:, 0],
        name="ts2",
    )
    # if 'uirevision' stays as it was before updating the figure, the zoom/ui will not reset. 'const' as value is arbitrary
    fig.add_scatter(x=ts1.time_column(), y=ts1.data_df().iloc[:, 0], name="ts1")
    aligner.add_visualization(fig)
    score = aligner.alignment_score()
    return f"{score}", fig


def default_ts1():
    ts = generate.generate_simple_with_noise(n=2000, dimensions=1)
    pipeline = Pipeline()
    pipeline.push(interpolate(factor=FACTOR)).push(index_to_time)
    ts = pipeline.apply(ts)
    return ts


def default_ts2():
    ts2 = generate.generate_simple_with_noise(n=2000, dimensions=1)
    pipeline = Pipeline()
    pipeline.push(add(4))
    ts2 = pipeline.apply(ts2)
    ts2.df = pd.DataFrame(ts2.df[OFFSET:]).dropna(axis="rows")
    return ts2


if __name__ == "__main__":
    app.run(debug=True)
