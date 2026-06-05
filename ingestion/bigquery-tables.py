from google.cloud import bigquery

client = bigquery.Client(project="energy-grid-pipeline")

# Creating the table
table_id = "energy-grid-pipeline.raw.prices"
schema = [
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
    bigquery.SchemaField("price", "FLOAT64"),
    bigquery.SchemaField("zone", "STRING"),
]

table = bigquery.Table(table_id, schema=schema)
table.time_partitioning = bigquery.TimePartitioning(
    type_=bigquery.TimePartitioningType.DAY,
    field="timestamp",
)

table = client.create_table(table, exists_ok=True)

print(
    f"Created table {table.project}.{table.dataset_id}.{table.table_id}, "
    f"partitioned on column {table.time_partitioning.field}."
)