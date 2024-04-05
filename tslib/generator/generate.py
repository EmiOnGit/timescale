from gutenTAG.gutenTAG import GutenTAG
import pandas as pd


def generate_simple_with_noise(n=40, dimensions=3, noise_strength=0.2, plot=False):
    anomalies = [
        {
            "exact-position": 0,
            "length": int(n),
            "channel": channel,
            "kinds": [{"kind": "variance", "variance": noise_strength}],
        }
        for channel in range(dimensions)
    ]
    config = {
        "timeseries": [
            {
                "name": "demo",
                "length": n,
                "channels": dimensions,
                "base-oscillation": {"kind": "sine"},
                "anomalies": anomalies,
            }
        ]
    }
    gutentag = GutenTAG()
    gutentag.load_config_dict(config)
    ts = gutentag.generate(return_timeseries=True, plot=plot)
    assert ts
    dfs = [ts.timeseries for ts in ts]
    for i, df in enumerate(dfs):
        df.drop("is_anomaly", axis=1, inplace=True)
        df.rename(columns={"value-0": f"value-{i}"}, inplace=True)
    res = pd.concat(dfs, axis=1)
    return res
