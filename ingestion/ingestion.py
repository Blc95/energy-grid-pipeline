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
from google.cloud import bigquery


env_path = Path(__file__).resolve().parent.parent / ".env"

load_dotenv(dotenv_path=env_path)


def build_gcs_path(country_code, date):
    return f"gs://energy-grid-pipeline-raw/prices/{country_code}/{date}/data.parquet"
    



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
    # Align with BigQuery's UTC-based DAY partitioning so each file maps to one partition.
    df["timestamp"] = df["timestamp"].dt.tz_convert("UTC")
    days = df.groupby(pd.Grouper(key='timestamp', freq='D'))
    
    for day, group in days:
        group = group[group["timestamp"].dt.normalize() == day]
        if group.empty:
            continue
        day_str = day.strftime('%Y-%m-%d')
        gcs_path = build_gcs_path(country_code=country_code, date=day_str)
        group.to_parquet(gcs_path)
     
     
        
def load_to_big_query(date, country_code):
    client = bigquery.Client(project="energy-grid-pipeline")
    
    job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    source_format=bigquery.SourceFormat.PARQUET,
    time_partitioning=bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="timestamp"
        )
    )
    
    date_nodashes = date.strftime("%Y%m%d")
    table_id = f"energy-grid-pipeline.raw.prices${date_nodashes}"
    
    uri = build_gcs_path(country_code=country_code, date=date.strftime("%Y-%m-%d"))
    
    load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))

def main():
    country_code = "DK_1"
    start = pd.Timestamp('20260520', tz='Europe/Brussels')
    end = pd.Timestamp('20260525', tz='Europe/Brussels')
    
    fetch_data(
        country_code=country_code,
        start=start,
        end=end
    ) 
    
    dates = pd.date_range(
        start=start.tz_convert("UTC").normalize(),
        end=end.tz_convert("UTC").normalize(),
        freq='D',
    )[:-1]
    
    for day in dates:
        print(day, day.strftime("%Y%m%d"))
        load_to_big_query(
            country_code=country_code,
            date=day
        )

if __name__ == "__main__":
    main()
