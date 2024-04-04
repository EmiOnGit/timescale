from gutenTAG import gutenTAG
from pathlib import Path


path = Path("test.yaml")
gutentag = gutenTAG.GutenTAG.from_yaml(path)
ts = gutentag.generate(return_timeseries=True)
ts = ts[0]
print("completed generation")
ts = ts.timeseries
print(ts.query("is_anomaly == 1"))
# print(f"len {n}")
print(ts)
