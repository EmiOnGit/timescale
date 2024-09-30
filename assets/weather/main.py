# Import Meteostat library
import numpy as np
import pandas as pd
from meteostat import Stations, Hourly
from datetime import datetime


start = datetime(2023, 1, 1)
end = datetime(2023, 12, 31, 23, 59)
marburg_castle = (50.810038774, 8.767037651)

# Get nearby weather stations
stations = Stations()
stations = stations.nearby(marburg_castle[0], marburg_castle[1])
stations = stations.fetch(30)
# filter stations that don't have any values
stations = stations[pd.notna(stations["daily_end"])]
station = stations[stations["distance"] < 100000]


data = Hourly(station, start=start, end=end)
data = data.fetch()
print(type(data))
print(data)


# Print DataFrame
# print(stations)
