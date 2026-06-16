from dagster import (asset,
                     Definitions,
                     DailyPartitionsDefinition,
                     StaticPartitionsDefinition,
                     MultiPartitionsDefinition,
                     build_schedule_from_partitioned_job,
                     define_asset_job)
import pandas as pd
import itertools

# Now import
from ingestion.ingestion import (
    fetch,
    fetch_generation,
    fetch_crossborder_flow,
    load_to_big_query,
    load_to_big_query_cross_border_flow
)

###### PARTITIONS ######

### FOR PRICE, LOAD, GENERATION ###
multi_partitions = MultiPartitionsDefinition({
    "date": DailyPartitionsDefinition(start_date="2025-10-01", timezone="Europe/Brussels"),
    "zone": StaticPartitionsDefinition(["DK_1", "DK_2"]),
})

### FOR CROSS BORDER FLOW ###
flow_pairs = ["DK_1__DK_2", "DK_2__DK_1"]   # or however you encode the directed pair

flow_partitions = MultiPartitionsDefinition({
    "date": DailyPartitionsDefinition(start_date="2025-10-01", timezone="Europe/Brussels"),
    "pair": StaticPartitionsDefinition(flow_pairs),
})


### LOAD AND PRICES ###
def build_price_and_load_dataset_assets(name, query_method, value_column, table):
    
    @asset(name=f"raw_{name}_gcs", partitions_def=multi_partitions)
    def gcs_asset(context):
        keys = context.partition_key.keys_by_dimension
        zone = keys["zone"]
        window = context.partition_time_window
        
        fetch(country_code=zone,
              start=pd.Timestamp(window.start),
              end=pd.Timestamp(window.end),
              query_method=query_method,      
              value_column=value_column,      
              bucket=name)                   
    
    @asset(name=f"raw_{name}_bigquery", deps=[f"raw_{name}_gcs"], partitions_def=multi_partitions)
    def bq_asset(context):
        keys = context.partition_key.keys_by_dimension
        zone = keys["zone"]
        window = context.partition_time_window
        
        load_to_big_query(country_code=zone,
                          date=pd.Timestamp(window.start),
                          table=table
                          )
    return [gcs_asset, bq_asset]


### GENERATION ###
def build_generation_dataset_assets(name, table):
    
    @asset(name=f"raw_{name}_gcs", partitions_def=multi_partitions)
    def gcs_asset(context):
        keys = context.partition_key.keys_by_dimension
        zone = keys["zone"]
        window = context.partition_time_window
        
        fetch_generation(
            country_code = zone,
            start = pd.Timestamp(window.start),
            end = pd.Timestamp(window.end),
            bucket = table
        )
    
    @asset(name=f"raw_{name}_bigquery", deps=[f"raw_{name}_gcs"], partitions_def=multi_partitions)
    def bq_asset(context):
        keys = context.partition_key.keys_by_dimension
        zone = keys["zone"]
        window = context.partition_time_window
        
        load_to_big_query(country_code=zone,
                        date=pd.Timestamp(window.start),
                        table=table
                        )
            
    return [gcs_asset, bq_asset]


### CROSS BORDER FLOW ###
def build_cross_border_flow_dataset_assets(name, table):
    
    @asset(name=f"raw_{name}_gcs", partitions_def=flow_partitions)
    def gcs_asset(context):
        keys = context.partition_key.keys_by_dimension
        pair = keys["pair"]                    # e.g. "DK_1__DK_2"
        zone_from, zone_to = pair.split("__")  # split into from/to
        window = context.partition_time_window
        fetch_crossborder_flow(
            country_code_from=zone_from,
            country_code_to=zone_to,
            start=pd.Timestamp(window.start),
            end=pd.Timestamp(window.end),
            bucket=table)
        
    @asset(name=f"raw_{name}_bigquery", deps=[f"raw_{name}_gcs"], partitions_def=flow_partitions)
    def bq_asset(context):
        keys = context.partition_key.keys_by_dimension
        pair = keys["pair"]                    # e.g. "DK_1__DK_2"
        zone_from, zone_to = pair.split("__")  # split into from/to
        window = context.partition_time_window
        
        load_to_big_query_cross_border_flow(
            country_code_from=zone_from,
            country_code_to=zone_to,
            date=pd.Timestamp(window.start),
            table=table
        )
    return [gcs_asset, bq_asset]
        
        

prices_assets = build_price_and_load_dataset_assets("prices", "query_day_ahead_prices", "price", "prices")
load_assets = build_price_and_load_dataset_assets("load", "query_load", "actual_load_mw", "load")
generation_assets = build_generation_dataset_assets("generation", "generation")
cross_border_flow_assets = build_cross_border_flow_dataset_assets("cross_border_flow", "cross_border_flow")



###### SCHEDULING #######

# PRICE, LOAD, GENERATION
partitioned_asset_job_price_load_generation = define_asset_job("partitioned_job_price_load_generation", selection=[*prices_assets, *load_assets, *generation_assets])


asset_partitioned_schedule_price_load_generation = build_schedule_from_partitioned_job(
    partitioned_asset_job_price_load_generation
)

# CROSS BORDER FLOW
partitioned_asset_job_cross_border_flow = define_asset_job("partitioned_job_cross_border_flow", selection=[*cross_border_flow_assets])

asset_partitioned_schedule_cross_border_flow = build_schedule_from_partitioned_job(
    partitioned_asset_job_cross_border_flow
)


###### DEFINITIONS ######
defs = Definitions(assets=[*prices_assets, *load_assets, *generation_assets, *cross_border_flow_assets],
                   schedules=[asset_partitioned_schedule_price_load_generation, asset_partitioned_schedule_cross_border_flow])


