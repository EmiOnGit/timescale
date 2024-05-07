from dash import html, dcc
import dash_bootstrap_components as dbc


def header(title) -> html.H1:
    return html.H1(
        title,
        style={
            "textAlign": "center",
            "display": "inline-block",
            "padding-right": "50px",
            "padding-left": "50px",
            "border-bottom": "2px solid #888",
        },
    )


def head_col(children):
    return dbc.Col(
        children,
        style={
            "display": "flex",
            "flex-direction": "column",
            "align-items": "center",
        },
    )


timeseries_block = head_col(
    [
        dcc.Upload(
            id=f"info_upload{i}",
            children=[
                header(f"ts{i}"),
                html.Div(id=f"info_n{i}", children="n="),
                html.Div(id=f"info_filepath{i}", children="default dataset"),
            ],
            style={
                "width": "80%",
                "lineHeight": "20px",
                "border": "2px dashed #ccc",
                "borderRadius": "5px",
                "textAlign": "center",
                "margin-right": "50px",
                "margin-left": "50px",
                "padding": "10px",
                "margin-bottom": "10px",
            },
            multiple=False,
        )
        for i in range(1, 3)
    ]
)
settings_block = head_col(
    [
        header("Settings"),
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.Div("score"),
                        html.Div("scale"),
                        html.Div("offset"),
                    ],
                    style={
                        "display": "flex",
                        "flex-direction": "column",
                        "align-items": "center",
                        "justify-content": "space-evenly",
                    },
                ),
                dbc.Col(
                    [
                        html.Div(id="align_score"),
                        dcc.Input(
                            id="input_scale",
                            type="number",
                            min=0.0,
                            max=5.0,
                            value=1.0,
                        ),
                        dcc.Input(
                            id="input_offset",
                            type="number",
                            min=-100000.0,
                            max=100000.0,
                            value=0.0,
                        ),
                    ],
                    style={
                        "display": "flex",
                        "flex-direction": "column",
                        "align-items": "center",
                        "justify-content": "space-evenly",
                    },
                ),
            ],
            style={"display": "flex", "flex-direction": "row", "align-items": "center"},
        ),
    ]
)
aligner_block = head_col(
    [
        header("Aligner"),
        dcc.Dropdown(
            ["correlation", "function sum", "eucl"],
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
                    min=0,
                    max=800,
                    step=1,
                    value=100,
                    marks={i: "{}".format(i) for i in range(0, 801, 100)},
                    # style={"width": "200px"},
                ),
                html.Div("iterations"),
                dcc.Slider(
                    id="slider_iterations",
                    min=0,
                    max=120,
                    step=1,
                    value=20,
                    marks={i: "{}".format(i) for i in range(0, 121, 20)},
                ),
            ],
            style={"width": "400px"},
        ),
        html.Button("Align", id="calculate_align"),
        html.Div(
            [
                html.Progress(id="progress_bar"),
                html.Button("Cancel", id="cancel_align_button"),
            ],
            id="align_progress_box",
            style={"visibility": "hidden"},
        ),
    ]
)


def layout():
    return html.Div(
        [
            html.H1(children="TimeScale", style={"textAlign": "center"}),
            dbc.Row(
                [
                    timeseries_block,
                    settings_block,
                    aligner_block,
                ]
            ),
            dcc.Graph(id="graph-content"),
            dcc.Store(id="ts1store"),
            dcc.Store(id="ts2store"),
            dcc.Store(id="alignment"),
            dcc.Store(id="settings"),
        ]
    )
