import atoti as tt
import pandas as pd
import time

df = pd.read_csv("us_states_preprocessed_with_ymd.csv")
df["date"] = pd.to_datetime(df["date"])

session = tt.Session.start()

covid_table = session.read_pandas(
    df,
    keys=["date", "state"],
    table_name="covid"
)

# Buat hierarchy sebagai list levels saja langsung pas create_cube
cube = session.create_cube(
    covid_table,
    hierarchies={
        "Date": [covid_table["year"], covid_table["month"], covid_table["day"]]
    }
)

m = cube.measures
m["sum_cases"] = tt.agg.sum(covid_table["cases"])
m["sum_deaths"] = tt.agg.sum(covid_table["deaths"])
m["case_fatality_rate (%)"] = (m["sum_deaths"] / m["sum_cases"]) * 100

print(session.link)

print("Press Ctrl+C to stop the server")
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Server stopped")
