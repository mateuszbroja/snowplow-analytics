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
        bigquery.SchemaField("derived_tstamp", "TIMESTAMP"),
        bigquery.SchemaField("event_name", "STRING"),
        bigquery.SchemaField("domain_userid", "STRING"),
        bigquery.SchemaField("domain_sessionidx", "INTEGER"),
        bigquery.SchemaField("geo_city", "STRING"),
        bigquery.SchemaField("app_id", "STRING"),
        bigquery.SchemaField("page_urlpath", "STRING"),
        bigquery.SchemaField("derived_tstamp", "TIMESTAMP"),  # duplicate column
        bigquery.SchemaField("dvce_created_tstamp", "TIMESTAMP"),
        bigquery.SchemaField("dvce_sent_tstamp", "TIMESTAMP"),
        bigquery.SchemaField("collector_tstamp", "TIMESTAMP"),
        bigquery.SchemaField("etl_tstamp", "TIMESTAMP"),
        bigquery.SchemaField("true_tstamp", "TIMESTAMP"),
        bigquery.SchemaField("user_id", "STRING"),
        bigquery.SchemaField("domain_userid", "STRING"),  # duplicate column
        bigquery.SchemaField("network_userid", "STRING"),
        bigquery.SchemaField("user_ipaddress", "STRING"),
        bigquery.SchemaField("user_fingerprint", "STRING"),
        bigquery.SchemaField("domain_sessionid", "STRING"),
        bigquery.SchemaField("domain_sessionidx", "INTEGER"),  # duplicate column
        bigquery.SchemaField("event_id", "STRING"),
        bigquery.SchemaField("event", "STRING"),
        bigquery.SchemaField("event_name", "STRING"),  # duplicate column
        bigquery.SchemaField("event_vendor", "STRING"),
        bigquery.SchemaField("event_format", "STRING"),
        bigquery.SchemaField("event_version", "STRING"),
        bigquery.SchemaField("event_fingerprint", "STRING"),
        bigquery.SchemaField("link_click_event", "STRING"),
        bigquery.SchemaField("focus_form_event", "STRING"),
        bigquery.SchemaField("change_form_event", "STRING"),
        bigquery.SchemaField("app_id", "STRING"),  # duplicate column
        bigquery.SchemaField("platform", "STRING"),
        bigquery.SchemaField("useragent", "STRING"),
        bigquery.SchemaField("useragent_parser_context", "STRING"),
        bigquery.SchemaField("yauaa_context", "STRING"),
        bigquery.SchemaField("iab_bot_context", "STRING"),
        bigquery.SchemaField("br_lang", "STRING"),
        bigquery.SchemaField("br_cookies", "BOOLEAN"),
        bigquery.SchemaField("br_colordepth", "INTEGER"),
        bigquery.SchemaField("doc_charset", "STRING"),
        bigquery.SchemaField("geo_country", "STRING"),
        bigquery.SchemaField("geo_region", "STRING"),
        bigquery.SchemaField("geo_region_name", "STRING"),
        bigquery.SchemaField("geo_city", "STRING"),  # duplicate column
        bigquery.SchemaField("geo_zipcode", "STRING"),
        bigquery.SchemaField("geo_latitude", "FLOAT"),
        bigquery.SchemaField("geo_longitude", "FLOAT"),
        bigquery.SchemaField("geo_timezone", "STRING"),
        bigquery.SchemaField("mkt_medium", "STRING"),
        bigquery.SchemaField("mkt_source", "STRING"),
        bigquery.SchemaField("mkt_term", "STRING"),
        bigquery.SchemaField("mkt_content", "STRING"),
        bigquery.SchemaField("mkt_campaign", "STRING"),
        bigquery.SchemaField("mkt_network", "STRING"),
        bigquery.SchemaField("mkt_clickid", "STRING"),
        bigquery.SchemaField("page_view_id", "STRING"),
        bigquery.SchemaField("page_url", "STRING"),
        bigquery.SchemaField("page_title", "STRING"),
        bigquery.SchemaField("page_urlscheme", "STRING"),
        bigquery.SchemaField("page_urlport", "INTEGER"),
        bigquery.SchemaField("page_urlhost", "STRING"),
        bigquery.SchemaField("page_urlpath", "STRING"),  # duplicate column
        bigquery.SchemaField("page_urlquery", "STRING"),
        bigquery.SchemaField("page_urlfragment", "STRING"),
        bigquery.SchemaField("performance_timing_context", "STRING"),
        bigquery.SchemaField("page_referrer", "STRING"),
        bigquery.SchemaField("refr_urlscheme", "STRING"),
        bigquery.SchemaField("refr_urlport", "INTEGER"),
        bigquery.SchemaField("refr_urlhost", "STRING"),
        bigquery.SchemaField("refr_urlpath", "STRING"),
        bigquery.SchemaField("refr_urlquery", "STRING"),
        bigquery.SchemaField("refr_urlfragment", "STRING"),
        bigquery.SchemaField("refr_medium", "STRING"),
        bigquery.SchemaField("refr_source", "STRING"),
        bigquery.SchemaField("refr_term", "STRING"),
        bigquery.SchemaField("refr_domain_userid", "STRING"),
        bigquery.SchemaField("refr_dvce_tstamp", "TIMESTAMP"),
        bigquery.SchemaField("br_viewwidth", "INTEGER"),
        bigquery.SchemaField("br_viewheight", "INTEGER"),
        bigquery.SchemaField("dvce_screenwidth", "INTEGER"),
        bigquery.SchemaField("dvce_screenheight", "INTEGER"),
        bigquery.SchemaField("doc_width", "INTEGER"),
        bigquery.SchemaField("doc_height", "INTEGER"),
        bigquery.SchemaField("pp_xoffset_min", "INTEGER"),
        bigquery.SchemaField("pp_xoffset_max", "INTEGER"),
        bigquery.SchemaField("pp_yoffset_min", "INTEGER"),
        bigquery.SchemaField("pp_yoffset_max", "INTEGER"),
        bigquery.SchemaField("v_tracker", "STRING"),
        bigquery.SchemaField("name_tracker", "STRING"),
        bigquery.SchemaField("v_collector", "STRING"),
        bigquery.SchemaField("v_etl", "STRING"),
    ]

    # Create dataset if not exists
    try:
        client.get_dataset(dataset_id)
    except Exception:
        dataset = bigquery.Dataset(f"{client.project}.{dataset_id}")
        dataset.location = "EU"
        client.create_dataset(dataset)

    # Create table if not exists
    try:
        client.get_table(full_table_id)
    except Exception:
        table = bigquery.Table(full_table_id, schema=schema)
        client.create_table(table)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    uri = f"gs://{bucket}/{file_name}"
    load_job = client.load_table_from_uri(uri, full_table_id, job_config=job_config)
    load_job.result()
