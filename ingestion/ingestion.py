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
import itertools
import logging

logger = logging.getLogger(__name__)

env_path = Path(__file__).resolve().parent.parent / ".env"

load_dotenv(dotenv_path=env_path)

def build_gcs_path(country_code, date, table):
    return f"gs://energy-grid-pipeline-raw/{table}/{country_code}/{date}/data.parquet"

def build_gcs_path_cross_border_flow(country_code_from, country_code_to, date, table):
    return f"gs://energy-grid-pipeline-raw/{table}/from_{country_code_from}_to_{country_code_to}/{date}/data.parquet"


########## FETCHING DATA FROM API ##########

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
        logger.info("Created gcs_path: %s", gcs_path) # Logging
        
### GENERATION ###
def fetch_generation(country_code, start, end, bucket):
    token = os.getenv("ENTSOE_TOKEN")
    if not token:
        raise ValueError("Error: ENTSOE_TOKEN not set in environment")
    client = EntsoePandasClient(api_key=token)
    data = client.query_generation(country_code, start=start, end=end, psr_type=None)
    
    df = data.reset_index()
    df.rename(columns={"index": "timestamp"}, inplace=True)
    df["zone"] = country_code
    df = df[df["timestamp"] < end]
    df["delivery_date"] = df["timestamp"].dt.tz_convert("Europe/Copenhagen").dt.date
    
    # Convert to datetime64
    df["timestamp"] = df["timestamp"].dt.tz_convert("UTC")
    
    melted_df = pd.melt(df,
            id_vars=["timestamp", "delivery_date", "zone"],
            var_name="fuel_type",
            value_name="generation_mw"
            )
    
    days = melted_df.groupby("delivery_date")
    
    for day, group in days:
        if group.empty:
            continue
        day_str = day.strftime('%Y-%m-%d')
        gcs_path = build_gcs_path(country_code=country_code, date=day_str, table=bucket)
        group.reset_index(drop=True).to_parquet(gcs_path)
        logger.info("Built gcs_path: %s", gcs_path) # Logging
        
### CROSS BORDER FLOW ###
def fetch_crossborder_flow(country_code_from, country_code_to, start, end, bucket):
    token = os.getenv("ENTSOE_TOKEN")
    
    if not token:
        raise ValueError("Error: ENTSOE_TOKEN not set in environment")
    client = EntsoePandasClient(api_key=token)
    data = client.query_crossborder_flows(country_code_from, country_code_to, start=start, end=end)

    df = data.reset_index()
    df.rename(columns={"index": "timestamp",
                       0: f"flow_mw"},
              inplace=True)
    
    df = df[df["timestamp"] < end]
    df["from_zone"] = country_code_from
    df["to_zone"] = country_code_to
    df["delivery_date"] = df["timestamp"].dt.tz_convert("Europe/Copenhagen").dt.date
    df["timestamp"] = df["timestamp"].dt.tz_convert("UTC")
    
    days = df.groupby("delivery_date")
    
    for day, group in days:
        if group.empty:
            continue
        day_str = day.strftime('%Y-%m-%d')
        gcs_path = build_gcs_path_cross_border_flow(country_code_from=country_code_from,
                                                    country_code_to = country_code_to,
                                                    date= day_str,
                                                    table=bucket)
        group.to_parquet(gcs_path)
        logger.info("Built gcs_path: %s", gcs_path)
     

######## LOADING TO BIGQUERY #######
     
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

    # Logging
    logger.info("Loaded %s rows to %s", load_job.output_rows,table_id_base)
    
    
def load_to_big_query_cross_border_flow(date, country_code_from, country_code_to, table):
    client = bigquery.Client(project="energy-grid-pipeline")
    
    # For DELETE QUERY
    table_id_base = f"energy-grid-pipeline.raw.{table}" 
    date_for_query = date.strftime(("%Y-%m-%d"))
    
    
    query = f"""
        DELETE FROM `{table_id_base}`
        WHERE 
            from_zone = @from_zone AND
            to_zone = @to_zone AND
            delivery_date = @delivery_date
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("from_zone", "STRING", country_code_from),
            bigquery.ScalarQueryParameter("to_zone", "STRING", country_code_to),
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

    day_str = date.strftime("%Y-%m-%d")
    uri = build_gcs_path_cross_border_flow(country_code_from=country_code_from,
                                                    country_code_to = country_code_to,
                                                    date= day_str,
                                                    table=table)

    
    load_job = client.load_table_from_uri(
    uri, table_id_base, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.
    
    # Logging
    logger.info("Loaded %s rows to %s", load_job.output_rows,table_id_base)

    
### MAIN ###
def main():
    
    # Logging
    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("entsoe").setLevel(logging.WARNING)
    
    
    country_codes = ["DK_1", "DK_2"]
    start = pd.Timestamp('20251025', tz='Europe/Brussels')
    end = pd.Timestamp('20251027', tz='Europe/Brussels')
    tables = ["prices", "load"]
    query_methods = ["query_day_ahead_prices", "query_load"]
    
    
    ### DEFINE DATE RANGE FOR IN GESTION ###
    dates = pd.date_range(
        start=start,
        end=end,
        freq='D',
    )[:-1]
    
    ### PRICES AND LOAD ###
    for query_method in query_methods:
        if query_method == "query_day_ahead_prices":
            bucket = "prices"
            value_column = "price"
        elif query_method == "query_load":
            bucket = "load"
            value_column = "actual_load_mw"
            
        for country_code in country_codes:
            try:
                fetch(country_code=country_code,
                    start=start,
                    end=end,
                    query_method=query_method,
                    value_column=value_column,
                    bucket=bucket)
            except Exception as e:
                logger.error("Failed fetching %s for %s: %s", bucket, country_code, e)
    
    ### LOADING PRICES AND LOAD ###
    for table in tables:
        for country_code in country_codes:
            for day in dates:
                try:
                    load_to_big_query(
                        country_code=country_code,
                        date=day,
                        table=table
                    )
                except Exception as e:
                    logger.error("Failed loading %s for %s for %s: %s", table, day, country_code, e)
  
    ### GENERATION ###
    g_table = "generation"
    
    for country_code in country_codes:
        try:
            fetch_generation(country_code=country_code,
                            start=start,
                            end=end,
                            bucket=g_table
                            )
        except Exception as e:
            logger.error("Failed fetching %s for %s: %s", g_table, country_code, e)
    
    
    ### LOADING GENERATION ###
    for country_code in country_codes:
        for day in dates:
            try:
                load_to_big_query(
                    country_code=country_code,
                    date=day,
                    table=g_table
                )
            except Exception as e:
                logger.error("Failed loading %s for %s for %s: %s", g_table, day, country_code, e)   
    

    ### CROSS BORDER FLOW ###
    cross_border_flow_table = "cross_border_flow"
    
    zone_permutations = list(itertools.permutations(country_codes, 2))

    for country_code_from, country_code_to in zone_permutations:
        try:
            fetch_crossborder_flow(
                country_code_from=country_code_from,
                country_code_to=country_code_to,
                start=start,
                end=end,
                bucket=cross_border_flow_table
            )
        except Exception as e:
            logger.error("Failed fetching %s from %s to %s: %s", cross_border_flow_table, country_code_from, country_code_to, e)
                
    # LOADING  CROSS BORDER FLOW INTO BIGQUEY
    for country_code_from, country_code_to in zone_permutations:
        for day in dates:
            try:
                load_to_big_query_cross_border_flow(
                    country_code_from=country_code_from,
                    country_code_to=country_code_to,
                    date=day,
                    table=cross_border_flow_table
                )
            except Exception as e:
                logger.error("Failed loading %s from %s to %s for %s: %s", cross_border_flow_table, country_code_from, country_code_to, day, e)
  

if __name__ == "__main__":
    main()
