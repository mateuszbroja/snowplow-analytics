from google.cloud import bigquery
import functions_framework


@functions_framework.cloud_event
def load_to_bq(cloud_event):
    bucket = cloud_event.data["bucket"]
    file_name = cloud_event.data["name"]

    client = bigquery.Client()
    dataset_id = "lego_tracking"
    table_id = "raw_events"
    full_table_id = f"lego-tracking-analytics.{dataset_id}.{table_id}"

    schema = [
        bigquery.SchemaField("Core", "STRING"),
        bigquery.SchemaField("Timestamps", "STRING"),
        bigquery.SchemaField("User", "STRING"),
        bigquery.SchemaField("Session", "STRING"),
        bigquery.SchemaField("Event", "STRING"),
        bigquery.SchemaField("Event_specific_fields", "STRING"),
        bigquery.SchemaField("Application", "STRING"),
        bigquery.SchemaField("Useragent", "STRING"),
        bigquery.SchemaField("Location", "STRING"),
        bigquery.SchemaField("Marketing", "STRING"),
        bigquery.SchemaField("Page", "STRING"),
        bigquery.SchemaField("Referer", "STRING"),
    ]

    # Check if dataset exists, if not create it
    try:
        client.get_dataset(dataset_id)
    except Exception:
        dataset = bigquery.Dataset(f"{client.project}.{dataset_id}")
        dataset.location = "EU"
        client.create_dataset(dataset)

    # Check if table exists, if not create it
    try:
        client.get_table(full_table_id)
    except Exception:
        table = bigquery.Table(full_table_id, schema=schema)
        client.create_table(table)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        schema=schema,
        allow_jagged_rows=True,
        ignore_unknown_values=True,
    )

    uri = f"gs://{bucket}/{file_name}"
    load_job = client.load_table_from_uri(uri, full_table_id, job_config=job_config)
    load_job.result()
