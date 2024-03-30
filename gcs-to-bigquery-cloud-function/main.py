import functions_framework 
from google.cloud import bigquery
from google.cloud import storage
import os

# Google Cloud Platform project ID
PROJECT_ID = "lct-dev-416808"
# Name of your BigQuery dataset
DATASET_ID = "dis_user_guide"
# Name of your BigQuery table
TABLE_ID = "temparature2"

# Initialize BigQuery client
bigquery_client = bigquery.Client(project=PROJECT_ID)

def create_table_if_not_exists():
    dataset_ref = bigquery_client.dataset(DATASET_ID)

    # Define the BigQuery table schema
    schema = [
        bigquery.SchemaField("item_name", "STRING"),
        bigquery.SchemaField("cost", "FLOAT"),
        bigquery.SchemaField("tax", "FLOAT"),
        bigquery.SchemaField("total", "FLOAT")
    ]

    table_ref = dataset_ref.table(TABLE_ID)
    table = bigquery.Table(table_ref, schema=schema)

    try:
        bigquery_client.get_table(table_ref)
        print(f"Table {DATASET_ID}.{TABLE_ID} already exists.")
    except Exception as e:
        # Create the table if it doesn't exist
        bigquery_client.create_table(table)
        print(f"Created table {DATASET_ID}.{TABLE_ID}.")

def load_csv_to_bigquery(bucket_name, file_name):
    dataset_ref = bigquery_client.dataset(DATASET_ID)
    table_ref = dataset_ref.table(TABLE_ID)

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1

    uri = f"gs://{bucket_name}/{file_name}"

    load_job = bigquery_client.load_table_from_uri(
        uri, table_ref, job_config=job_config
    )

    load_job.result()  # Waits for the job to complete.

    print(f"Loaded {load_job.output_rows} rows into {DATASET_ID}.{TABLE_ID}.")

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def handle_gcs_event(cloud_event):
    data = cloud_event.data

    bucket = data["bucket"]
    name = data["name"]

    print(f"File: {name} uploaded to {bucket}.")

    # Check if the uploaded file is a CSV
    if name.endswith('.csv'):
        # Create the table if it doesn't exist
        create_table_if_not_exists()
        # Load the CSV file into BigQuery
        load_csv_to_bigquery(bucket, name)

    else:
        print("Uploaded file is not a CSV, ignoring.")

    return "Done" 