import json
from io import BytesIO, StringIO

from pytest import mark

from dcraft.domain.type.enum import ContentType

try:
    import pandas as pd
    from google.cloud.storage import Client

    from dcraft.interface.data.gcs import GcsDataRepository
except ModuleNotFoundError:
    pass

from ...setting import GCP_PROJECT, GCS_BUCKET

# TODO: Fix test dependency


@mark.integration
def test_init():
    data_repository = GcsDataRepository(GCP_PROJECT, GCS_BUCKET)
    assert data_repository._bucket_name == GCS_BUCKET


@mark.integration
def test_save_dict():
    data_repository = GcsDataRepository(GCP_PROJECT, GCS_BUCKET)
    content = {"a": 2, "b": 2}
    data_repository.save(
        content, "test-project", "raw", "test-id", "json", ContentType.DICT
    )
    client = Client(GCP_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)
    blob = bucket.blob("test-project/raw/test-id.json")
    saved_content = blob.download_as_text()
    assert json.loads(saved_content) == content


@mark.integration
def test_load_dict():
    data_repository = GcsDataRepository(GCP_PROJECT, GCS_BUCKET)
    loaded_content = data_repository.load(
        "test-project", "raw", "test-id", "json", ContentType.DICT
    )
    expected_content = {"a": 2, "b": 2}
    assert loaded_content == expected_content


@mark.integration
def test_save_csv():
    data_repository = GcsDataRepository(GCP_PROJECT, GCS_BUCKET)
    content = pd.DataFrame({"a": [1], "b": [2]})
    data_repository.save(
        content, "test-project", "raw", "test-id", "csv", ContentType.DF
    )
    client = Client(GCP_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)
    blob = bucket.blob("test-project/raw/test-id.csv")
    saved_content = blob.download_as_text()
    assert pd.read_csv(StringIO(saved_content)).equals(content)


@mark.integration
def test_load_csv():
    data_repository = GcsDataRepository(GCP_PROJECT, GCS_BUCKET)
    loaded_content = data_repository.load(
        "test-project", "raw", "test-id", "csv", ContentType.DF
    )
    expected_content = pd.DataFrame({"a": [1], "b": [2]})
    assert loaded_content.equals(expected_content)


@mark.integration
def test_save_parquet():
    data_repository = GcsDataRepository(GCP_PROJECT, GCS_BUCKET)
    content = pd.DataFrame({"a": [1], "b": [2]})
    data_repository.save(
        content, "test-project", "raw", "test-id", "parquet", ContentType.DF
    )
    client = Client(GCP_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)
    blob = bucket.blob("test-project/raw/test-id.parquet")
    saved_content = blob.download_as_bytes()
    assert pd.read_parquet(BytesIO(saved_content)).equals(content)


@mark.integration
def test_load_parquet():
    data_repository = GcsDataRepository(GCP_PROJECT, GCS_BUCKET)
    loaded_content = data_repository.load(
        "test-project", "raw", "test-id", "parquet", ContentType.DF
    )
    expected_content = pd.DataFrame({"a": [1], "b": [2]})
    assert loaded_content.equals(expected_content)
