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

def build_gcs_path(country_code, date, table):
    return f"gs://energy-grid-pipeline-raw/{table}/{country_code}/{date}/data.parquet"


def fetch(country_code, start, end, query_method, value_column, bucket):
    token = os.getenv("ENTSOE_TOKEN")
    if not token:
        raise ValueError("Error: ENTSOE_TOKEN not set in environment")
    
    client = EntsoePandasClient(api_key=token)
    
    method = getattr(client, query_method)
    data = method(
        country_code=country_code,
        start=start,
        end=end
    )
    
    df = data.reset_index()
    df.columns = ["timestamp", value_column]
    df["zone"] = country_code
    df = df[df["timestamp"] < end]
    
    df["delivery_date"] = df["timestamp"].dt.tz_convert("Europe/Copenhagen").dt.date
    df["timestamp"] = df["timestamp"].dt.tz_convert("UTC")
    
    days = df.groupby("delivery_date")
    
    for day, group in days:
        if group.empty:
            continue
        day_str = day.strftime('%Y-%m-%d')
        gcs_path = build_gcs_path(country_code=country_code, date=day_str, table=bucket)
        group.to_parquet(gcs_path)
     
     
def load_to_big_query(date, country_code, table):
    client = bigquery.Client(project="energy-grid-pipeline")
    
    # For DELETE QUERY
    table_id_base = f"energy-grid-pipeline.raw.{table}" 
    date_for_query = date.strftime(("%Y-%m-%d"))
    
    query = f"""
        DELETE FROM `{table_id_base}`
        WHERE zone = @zone 
        AND delivery_date = @delivery_date
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("zone", "STRING", country_code),
            bigquery.ScalarQueryParameter("delivery_date", "DATE", date_for_query),
        ]
    )

    job = client.query(query, job_config=job_config)
    job.result()
    
    job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    source_format=bigquery.SourceFormat.PARQUET,
    time_partitioning=bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="delivery_date"
        )
    )
    
    uri = build_gcs_path(country_code=country_code,
                         date=date.strftime("%Y-%m-%d"),
                         table=table)
    
    
    load_job = client.load_table_from_uri(
    uri, table_id_base, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id_base)
    print("Loaded {} rows.".format(destination_table.num_rows))

def main():
    country_codes = ["DK_1", "DK_2"]
    start = pd.Timestamp('20251025', tz='Europe/Brussels')
    end = pd.Timestamp('20251027', tz='Europe/Brussels')
    tables = ["prices", "load"]
    query_methods = ["query_day_ahead_prices", "query_load"]
    
    # for query_method in query_methods:
    #     if query_method == "query_day_ahead_prices":
    #         bucket = "prices"
    #         value_column = "price"
    #     elif query_method == "query_load":
    #         bucket = "load"
    #         value_column = "actual_load_mw"
            
    #     for country_code in country_codes:
    #         fetch(country_code=country_code,
    #             start=start,
    #             end=end,
    #             query_method=query_method,
    #             value_column=value_column,
    #             bucket=bucket)
    
    dates = pd.date_range(
        start=start,
        end=end,
        freq='D',
    )[:-1]

    for table in tables:
        for country_code in country_codes:
            for day in dates:
                print(day, day.strftime("%Y%m%d"))
                print(country_code)
                load_to_big_query(
                    country_code=country_code,
                    date=day,
                    table=table
                )

if __name__ == "__main__":
    main()
