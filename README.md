# Snowplow Analytics Project

---

## Project Overview

This project processes and analyzes user behavior data from Snowplow sample data.

### Technology Used

- `Google Cloud Platform`
- `BigQuery`/`dbt`
- `Cloud Functions`
- `Google Cloud Storage`
- `Google Sheets`
- `GitHub Actions`

## Data Products

1. **BigQuery Tables**
   - `raw_events`: Raw Snowplow data
   - `staging_snowplow_events`: Cleaned event data
   - `dim_users`: User dimensions and metrics
   - `fct_sessions`: Session-level facts
   - `user_engagement`: Analytics and insights

2. **Connected Google Sheets**
   - Mart Tables Spreadsheets (connected to `dim_users` and `fct_sessions`): [Google Sheet](https://docs.google.com/spreadsheets/d/1SzLXdHWpabiICFUMr4IgQg3PPa4eYQI92yaGMj3I934/edit?usp=sharing)

## Architecture

```mermaid
graph LR
    A[Snowplow Data] -->|Upload| B[Cloud Storage]
    B -->|Trigger| C[Cloud Function]
    C -->|Load| D[BigQuery Raw]
    D -->|Transform| E[DBT Models]
    E -->|Analytics| F[Google Sheets]
    
    subgraph "DBT Transformations"
    E1[Staging] --> E2[Marts]
    E2 --> E3[Analytics]
    end

    subgraph "Orchestration"
    G[GitHub Actions] -->|Scheduled Runs| E
    G -->|Tests| E
    end
```

### Snowplow

*(not a part of the project)*

Generates raw behavioral data (clicks, page views, custom events) in JSON format and regularly uploads it to Google Cloud Storage.

### Cloud Storage

Acts as a landing zone for raw data files. New file arrivals automatically trigger the next processing step.

### Cloud Functions

Automatically processes new data files from buckets and loads them into BigQuery tables. It also creates a dataset and table if needed.

### BigQuery

Data warehouse hosting both raw and processed datasets. Raw data tables feed into dbt transformations, which output processed analytical tables.

### DBT

Handles data transformations in three stages:

- Staging: Data cleaning
- Marts: Business logic
- Analytics: Final metrics

Includes data quality tests throughout the pipeline.

### Google Sheets

Displays processed data for reporting and visualization.

### GitHub Actions

Automates dbt tests and transformations with daily runs at 5 AM UTC.

---

## Setup Instructions

### 1. GCP Setup

```bash
# Login to GCP
gcloud auth login
gcloud auth application-default login

# Create project
gcloud projects create lego-tracking-analytics
gcloud config set project lego-tracking-analytics

# Enable billing (do this in GCP Console)

# Create buckets
gsutil mb -l EU gs://lego-tracking-raw
gsutil mb -l EU gs://lego-tracking-dbt-logs

# Create dataset
bq mk --dataset lego_tracking

# After Cloud Function deployment - load the CSV file to the bucket
gsutil cp snowplow_sample.csv gs://lego-tracking-raw/
```

### 2. DBT Setup

```bash
# Install DBT
pip install dbt-bigquery

# Verify installation
dbt --version

# Initialize project
dbt init dbt_snowplow_analytics

# Test connection
dbt debug
```

> <img width="739" alt="image" src="https://github.com/user-attachments/assets/b42ef73d-aeca-4c1f-988e-0f2fb9d1a680" />

### 3. Cloud Function Setup

```bash
# Make scripts executable
chmod +x cloud_function/deploy.sh
chmod +x cloud_function/enable_apis.sh
chmod +x cloud_function/iam.sh

# Enable APIs and set up IAM
./cloud_function/enable_apis.sh
./cloud_function/iam.sh

# Deploy function
./cloud_function/deploy.sh
```

> <img width="794" alt="image" src="https://github.com/user-attachments/assets/db291e60-950b-4bed-b6ee-8c67d96a96b3" style="border: 1px solid #ddd; border-radius: 4px; padding: 5px;"/>

### 4. GitHub Actions Setup

1. Create secrets in repository:
   - `GCP_CREDENTIALS`: `Service account JSON`
   - `GCP_PROJECT_ID`: `lego-tracking-analytics`

> <img width="646" alt="image" src="https://github.com/user-attachments/assets/773a40f5-5652-4187-a206-f5b3733ce394" />

## Project Structure

```
.
├── README.md
├── .github/                # GitHub Configuration
│   └── workflows/          # GitHub Actions Workflows
│       └── dbt.yml         # DBT pipeline configuration
├── cloud_function/         # Cloud Function code
├── dbt_snowplow_analytics/      # DBT project
└── sample_data/            # Test data
```

### Cloud Function Files

- `main.py`: Loads data from Cloud Storage to BigQuery
- `requirements.txt`: Python dependencies
- `deploy.sh`: Deployment script
- `enable_apis.sh`: Enables required GCP APIs
- `iam.sh`: Sets up permissions

### DBT Models

1. Staging Layer (`models/staging/`)
   - `stg_snowplow_events.sql`: Cleans raw events data
   - `schema.yml`: Column definitions and tests

2. Marts Layer (`models/marts/`)
   - `dim_users.sql`: User dimensions
   - `fct_sessions.sql`: Session metrics

3. Analytics Layer (`models/analytics/`)
   - `user_engagement.sql`: User behavior analysis

> <img width="310" alt="image" src="https://github.com/user-attachments/assets/c0085877-c4a8-409f-9942-b1d490a57fc0" />

## Future Extensions

1. Data Quality
   - Add more data quality tests
   - Implement data freshness checks
   - Add unit tests and E2E testing

2. Analytics
   - More models/view
   - Dashboards
   - ML and AI magic

3. Infrastructure
   - Monitoring and alerting
   - Data backup strategy
   - Performance optimization
   - Cost monitoring
