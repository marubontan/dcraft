import os

GCP_PROJECT = os.getenv("GCP_PROJECT", None)
GCS_BUCKET = os.getenv("GCS_BUCKET", None)
BQ_DATASET_ID = "test_dataset"
BQ_TABLE_ID = "test_table"
