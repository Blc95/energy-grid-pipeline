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


env_path = Path(__file__).resolve().parent.parent / ".env"

load_dotenv(dotenv_path=env_path)


def fetch_data(country_code, start, end):
    client = EntsoePandasClient(api_key=os.getenv("ENTSOE_TOKEN"))
    
    if not client:
        raise ValueError("Error: ENTSOE_TOKEN not set in enviroment")
    
    start = start
    end = end
    country_code = country_code
    
    data = client.query_day_ahead_prices(country_code=country_code, start=start, end=end)
    
    df = data.reset_index()
    df.columns = ["timestamp", "price"]
    df["zone"] = country_code
    df = df.iloc[:-1, :]
    
    return df

def main():
    
    df_data = fetch_data(
        country_code='DK_1',
        start=pd.Timestamp('20260520', tz='Europe/Brussels'),
        end=pd.Timestamp('20260521', tz='Europe/Brussels')
        )
    
    print(df_data.head(10))
    print(df_data.tail(10))
    print(df_data.shape)
    print(df_data.columns)
    
    df_data.to_parquet("prices.parquet")

if __name__ == "__main__":
    main()
