PROJECT_NUMBER=$(gcloud projects describe lego-tracking-analytics --format='value(projectNumber)')

gcloud projects add-iam-policy-binding lego-tracking-analytics \
  --member="serviceAccount:service-${PROJECT_NUMBER}@gs-project-accounts.iam.gserviceaccount.com" \
  --role="roles/pubsub.publisher"

gcloud projects add-iam-policy-binding lego-tracking-analytics \
  --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
  --role="roles/eventarc.eventReceiver"