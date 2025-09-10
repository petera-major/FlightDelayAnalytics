import os
import pandas as pd
from sqlalchemy import create_engine

# local global connection
DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASS", "")  
DB_HOST = os.getenv("DB_HOST", "")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME", "flightdb")

if DB_PASS:
    CONN_STR = f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
else:
    CONN_STR = f"mysql+mysqlconnector://{DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(CONN_STR, pool_pre_ping=True)

def norm(cols):
    return [c.strip().lower().replace(" ", "_").replace("/", "_") for c in cols]

def to_dt(series, fmt=None):
    if fmt:
        return pd.to_datetime(series, format=fmt, errors="coerce")
    return pd.to_datetime(series, errors="coerce")

# load departures.csv
def load_flights(csv_path: str):
    df = pd.read_csv(csv_path)
    df.columns = norm(df.columns)

    # flight_date
    if "date_(mm_dd_yyyy)" in df.columns:
        df["flight_date"] = to_dt(df["date_(mm_dd_yyyy)"], fmt="%m/%d/%Y")

    # Parse actual times if present
    for col in ("scheduled_departure_time", "actual_departure_time"):
        if col in df.columns:
            df[col] = to_dt(df[col])

    # Rename verbose columns
    ren = {
        "departure_delay_(minutes)": "dep_delay_minutes",
        "delay_carrier_(minutes)": "delay_carrier_minutes",
        "delay_weather_(minutes)": "delay_weather_minutes",
        "delay_national_aviation_system_(minutes)": "delay_nas_minutes",
        "delay_security_(minutes)": "delay_security_minutes",
        "delay_late_aircraft_arrival_(minutes)": "delay_late_aircraft_minutes"
    }
    df.rename(columns={k: v for k, v in ren.items() if k in df.columns}, inplace=True)

    df.to_sql("flights_raw", con=engine, if_exists="replace", index=False)
    return len(df)

# load weather.csv 
def load_weather(csv_path: str):
    w = pd.read_csv(csv_path)
    w.columns = norm(w.columns)
    if "date" in w.columns:
        w["date"] = to_dt(w["date"])  # e.g., 2023-01-31

    keep = [c for c in ["station","name","date","prcp","tmax","tmin","awnd","wsf2"] if c in w.columns]
    w = w[keep]
    w.to_sql("weather_daily", con=engine, if_exists="replace", index=False)
    return len(w)

if __name__ == "__main__":
    flights_csv = os.getenv("FLIGHTS_CSV", "departures.csv")
    weather_csv = os.getenv("WEATHER_CSV", "weather.csv")

    n_f = load_flights(flights_csv)
    n_w = load_weather(weather_csv)
    print(f"Loaded {n_f} rows into flights_raw and {n_w} rows into weather_daily in database '{DB_NAME}'.")