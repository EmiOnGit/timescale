from gutenTAG.gutenTAG import GutenTAG
import pandas as pd
# from pathlib import Path


# path = Path("test.yaml")
# gutentag = gutenTAG.GutenTAG.from_yaml(path)
# ts = gutentag.generate(return_timeseries=True)
# if ts:
#     n = len(ts)
#     print("len ", n)
#     ts = ts[0]
#     print("completed generation")
#     ts = ts.timeseries
#     print(ts.query("is_anomaly == 1"))
# # print(f"len {n}")
# print(ts)


def generate_simple_with_anomalies(
    n=20,
    dimensions=3,
):
    config = {
        "timeseries": [
            {
                "name": "demo",
                "length": n,
                "base-oscillations": [{"kind": "sine"}],
                "anomalies": [
                    {
                        # "position": "middle",
                        "length": int(n / 2),
                        "channel": 0,
                        "kinds": [{"kind": "amplitude", "amplitude_factor": 2.0}],
                    }
                ],
            }
            for _ in range(dimensions)
        ]
    }
    gutentag = GutenTAG()
    gutentag.load_config_dict(config)
    ts = gutentag.generate(return_timeseries=True)
    assert ts
    dfs = [ts.timeseries for ts in ts]
    for i, df in enumerate(dfs):
        df.drop("is_anomaly", axis=1, inplace=True)
        df.rename(columns={"value-0": f"value-{i}"}, inplace=True)
    res = pd.concat(dfs, axis=1)
    # for df in dfs[1:]:

    return res


# generate_simple_with_anomalies()
