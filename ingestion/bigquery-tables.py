from google.cloud import bigquery

client = bigquery.Client(project="energy-grid-pipeline")

# Creating the table
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