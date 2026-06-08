from google.cloud import bigquery

client = bigquery.Client(project="energy-grid-pipeline")

### PRICES TABLE ###
table_id_prices = "energy-grid-pipeline.raw.prices"
schema_prices = [
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
    bigquery.SchemaField("delivery_date", "DATE"),
    bigquery.SchemaField("price", "FLOAT64"),
    bigquery.SchemaField("zone", "STRING"),
]

table_prices = bigquery.Table(table_id_prices, schema=schema_prices)
table_prices.time_partitioning = bigquery.TimePartitioning(
    type_=bigquery.TimePartitioningType.DAY,
    field="delivery_date",
)

table_prices = client.create_table(table_prices, exists_ok=True)

print(
    f"Created table {table_prices.project}.{table_prices.dataset_id}.{table_prices.table_id}, "
    f"partitioned on column {table_prices.time_partitioning.field}."
)


### LOAD TABLE ###
table_id_load = "energy-grid-pipeline.raw.load"

schema_load = [
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
    bigquery.SchemaField("delivery_date", "DATE"),
    bigquery.SchemaField("actual_load_mw", "FLOAT64"),
    bigquery.SchemaField("zone", "STRING"),
]

table_load = bigquery.Table(table_id_load, schema=schema_load)
table_load.time_partitioning = bigquery.TimePartitioning(
    type_=bigquery.TimePartitioningType.DAY,
    field="delivery_date",
)

table_load = client.create_table(table_load, exists_ok=True)

print(
    f"Created table {table_load.project}.{table_load.dataset_id}.{table_load.table_id}, "
    f"partitioned on column {table_load.time_partitioning.field}."
)

### GENERATION TABLE ###
table_id_generation = "energy-grid-pipeline.raw.generation"

schema_generation = [
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
    bigquery.SchemaField("delivery_date", "DATE"),
    bigquery.SchemaField("fuel_type", "STRING"),
    bigquery.SchemaField("generation_mw", "FLOAT64"),
    bigquery.SchemaField("zone", "STRING"),
]

table_generation = bigquery.Table(table_id_generation, schema=schema_generation)
table_generation.time_partitioning = bigquery.TimePartitioning(
    type_=bigquery.TimePartitioningType.DAY,
    field="delivery_date",
)

table_generation = client.create_table(table_generation, exists_ok=True)

print(
    f"Created table {table_generation.project}.{table_generation.dataset_id}.{table_generation.table_id}, "
    f"partitioned on column {table_generation.time_partitioning.field}."
)


### CROSS BORDER FLOW TABLE ###
table_id_cross_border_flow = "energy-grid-pipeline.raw.cross_border_flow"

schema_cross_border_flow = [
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
    bigquery.SchemaField("delivery_date", "DATE"),
    bigquery.SchemaField("from_zone", "STRING"),
    bigquery.SchemaField("to_zone", "STRING"),
    bigquery.SchemaField("flow_mw", "FLOAT64"),
]

table_cross_border_flow = bigquery.Table(table_id_cross_border_flow, schema=schema_cross_border_flow)
table_cross_border_flow.time_partitioning = bigquery.TimePartitioning(
    type_=bigquery.TimePartitioningType.DAY,
    field="delivery_date",
)

table_cross_border_flow = client.create_table(table_cross_border_flow, exists_ok=True)

print(
    f"Created table {table_cross_border_flow.project}.{table_cross_border_flow.dataset_id}.{table_cross_border_flow.table_id}, "
    f"partitioned on column {table_cross_border_flow.time_partitioning.field}."
)