gcloud functions deploy snowplow_load \
  --gen2 \
  --runtime=python310 \
  --region=europe-west1 \
  --source=. \
  --entry-point=load_to_bq \
  --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
  --trigger-event-filters="bucket=lego-tracking-raw" \
  --trigger-location=eu