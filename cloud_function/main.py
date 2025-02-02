from google.cloud import bigquery
import functions_framework


@functions_framework.cloud_event
def load_to_bq(cloud_event):
    bucket = cloud_event.data["bucket"]
    file_name = cloud_event.data["name"]

    client = bigquery.Client()
    table_id = "lego-tracking-analytics.lego_tracking.raw_events"

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

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        schema=schema,
        allow_jagged_rows=True,
        ignore_unknown_values=True,
    )

    uri = f"gs://{bucket}/{file_name}"
    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()
