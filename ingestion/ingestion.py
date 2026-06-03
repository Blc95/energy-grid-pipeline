from requests import Session
import psycopg2
import os
import sys
import logging
from entsoe import EntsoePandasClient
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path
import pyarrow
from datetime import timedelta
from google.cloud import storage



env_path = Path(__file__).resolve().parent.parent / ".env"

load_dotenv(dotenv_path=env_path)


def fetch_data(country_code, start, end):
    token = os.getenv("ENTSOE_TOKEN")
    
    if not token:
        raise ValueError("Error: ENTSOE_TOKEN not set in environment")
    
    client = EntsoePandasClient(api_key=token)
    
    data = client.query_day_ahead_prices(country_code=country_code,
                                         start=start,
                                         end=end
                                         )
    
    df = data.reset_index()
    df.columns = ["timestamp", "price"]
    df["zone"] = country_code
    df = df[df["timestamp"] < end]
    days = df.groupby(pd.Grouper(key='timestamp', freq='D'))
    
    for day, group in days:
        day_str = day.strftime('%Y-%m-%d')
        gcs_path = f"gs://energy-grid-pipeline-raw/prices/{country_code}/{day_str}/data.parquet"
        group.to_parquet(gcs_path)

def main():
    
    fetch_data(
        country_code='DK_1',
        start=pd.Timestamp('20260520', tz='Europe/Brussels'),
        end=pd.Timestamp('20260522', tz='Europe/Brussels')
    )

if __name__ == "__main__":
    main()
