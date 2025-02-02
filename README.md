# Ścieżka Data Engineering

https://snowplow.io/explore-snowplow-data-part-1

gcloud auth login
gcloud projects create lego-tracking-analytics
gcloud auth application-default login
gcloud config set project lego-tracking-analytics

Now you need to enable and link the billing account

Create Storage Buckets:
gsutil mb -l EU gs://lego-tracking-raw
gsutil mb -l EU gs://lego-tracking-dbt-logs

Create BigQuery Dataset:
bq mk --dataset lego_tracking


pip install dbt-bigquery
dbt --version
dbt init lego_tracking_dbt
dbt debug


chmod +x deploy.sh
chmod +x enable_apis.sh



Project Name: lego-tracking-analytics
Bucket Names: 
- Raw data: lego-tracking-raw
- DBT logs: lego-tracking-dbt-logs
BigQuery Dataset: lego_tracking


GCP_CREDENTIALS
GCP_PROJECT_ID

lego-tracking-analytics
