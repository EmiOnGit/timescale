from dash import html, dcc
import dash_bootstrap_components as dbc

timeseries_block = [
    dbc.Col(
        [
            dcc.Upload(
                id=f"info_upload{i}",
                children=[
                    html.H1(f"ts{i}", style={"textAlign": "center"}),
                    html.Hr(),
                    html.Div(id=f"info_n{i}", children="n="),
                    html.Div(id=f"info_filepath{i}", children="default dataset"),
                ],
                style={
                    "width": "80%",
                    "lineHeight": "20px",
                    "borderWidth": "1px",
                    "borderStyle": "dashed",
                    "borderRadius": "5px",
                    "textAlign": "center",
                    "margin-right": "50px",
                    "margin-left": "50px",
                    "padding-right": "20px",
                    "padding-left": "20px",
                },
                multiple=False,
            ),
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


def layout():
    return html.Div(
        [
            html.H1(children="TimeScale", style={"textAlign": "center"}),
            dbc.Row([timeseries_block[0], timeseries_block[1], settings_block]),
            html.Div(id="align_score", style={"whiteSpace": "pre-line"}),
            dcc.Graph(id="graph-content"),
            html.Plaintext(children="scale", style={"textAlign": "center"}),
            dcc.Input(
                id="input_scale",
                type="number",
                min=0.0,
                max=5.0,
                value=1.0,
            ),
            html.Div(
                [
                    html.Plaintext(children="offset", style={"textAlign": "center"}),
                    dcc.Input(
                        id="input_offset",
                        type="number",
                        min=-10000.0,
                        max=10000.0,
                        value=0.0,
                    ),
                ],
            ),
            dcc.Store(id="ts1store"),
            dcc.Store(id="ts2store"),
            dcc.Store(id="alignment"),
            dcc.Store(id="settings"),
        ]
    )
