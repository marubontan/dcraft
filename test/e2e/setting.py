import os

GCP_PROJECT = os.getenv("GCP_PROJECT", None)
GCS_BUCKET = os.getenv("GCS_BUCKET", None)
BQ_DATASET_ID = os.getenv("BQ_DATASET_ID", "test_dataset")
BQ_TABLE_ID = os.getenv("BQ_TABLE_ID", "test_table")
MONGO_HOST = os.getenv("MONGO_HOST", None)
MONGO_PORT = os.getenv("MONGO_PORT", 27017)
MONGO_DB = os.getenv("MONGO_DB", "test_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "test_collection")
