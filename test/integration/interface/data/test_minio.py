import json
from io import BytesIO, StringIO

from pytest import mark

from dcraft.domain.type.enum import ContentType

try:
    import pandas as pd
    from minio import Minio

    from dcraft.interface.data.minio import MinioRepository
except ModuleNotFoundError:
    pass

from ...setting import MINIO_ACCESS_KEY, MINIO_BUCKET, MINIO_ENDPOINT, MINIO_SECRET_KEY

# TODO: Fix test dependency


@mark.integration
def test_init():
    data_repository = MinioRepository(
        MINIO_ENDPOINT, MINIO_BUCKET, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
    )
    assert data_repository._bucket == MINIO_BUCKET


@mark.integration
def test_save_dict_list_json():
    data_repository = MinioRepository(
        MINIO_ENDPOINT, MINIO_BUCKET, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False
    )
    content = [{"a": 2, "b": 2}, {"a": 2, "b": 2}]
    data_repository.save(
        content, "test-project", "raw", "test-id", "json", ContentType.DICT
    )
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    saved_content = client.get_object(
        MINIO_BUCKET, "test-project/raw/test-id.json"
    ).read()
    assert json.loads(saved_content) == content


@mark.integration
def test_load_dict_list_json():
    data_repository = MinioRepository(
        MINIO_ENDPOINT, MINIO_BUCKET, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False
    )
    loaded_content = data_repository.load(
        "test-project", "raw", "test-id", "json", ContentType.DICT
    )
    expected_content = [{"a": 2, "b": 2}, {"a": 2, "b": 2}]
    assert loaded_content == expected_content


@mark.integration
def test_save_dict_json():
    data_repository = MinioRepository(
        MINIO_ENDPOINT, MINIO_BUCKET, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False
    )
    content = {"a": 2, "b": 2}
    data_repository.save(
        content, "test-project", "raw", "test-id", "json", ContentType.DICT
    )
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    saved_content = client.get_object(
        MINIO_BUCKET, "test-project/raw/test-id.json"
    ).read()
    assert json.loads(saved_content) == content


@mark.integration
def test_load_dict_json():
    data_repository = MinioRepository(
        MINIO_ENDPOINT, MINIO_BUCKET, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False
    )
    loaded_content = data_repository.load(
        "test-project", "raw", "test-id", "json", ContentType.DICT
    )
    expected_content = {"a": 2, "b": 2}
    assert loaded_content == expected_content


@mark.integration
def test_save_df_csv():
    data_repository = MinioRepository(
        MINIO_ENDPOINT, MINIO_BUCKET, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False
    )
    content = pd.DataFrame({"a": [1], "b": [2]})
    data_repository.save(
        content, "test-project", "raw", "test-id", "csv", ContentType.DF
    )
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    saved_content = client.get_object(
        MINIO_BUCKET, "test-project/raw/test-id.csv"
    ).read()
    assert pd.read_csv(StringIO(saved_content.decode("utf-8"))).equals(content)


@mark.integration
def test_load_df_csv():
    data_repository = MinioRepository(
        MINIO_ENDPOINT, MINIO_BUCKET, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False
    )
    loaded_content = data_repository.load(
        "test-project", "raw", "test-id", "csv", ContentType.DF
    )
    expected_content = pd.DataFrame({"a": [1], "b": [2]})
    assert loaded_content.equals(expected_content)


@mark.integration
def test_save_df_parquet():
    data_repository = MinioRepository(
        MINIO_ENDPOINT, MINIO_BUCKET, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False
    )
    content = pd.DataFrame({"a": [1], "b": [2]})
    data_repository.save(
        content, "test-project", "raw", "test-id", "parquet", ContentType.DF
    )
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    saved_content = client.get_object(
        MINIO_BUCKET, "test-project/raw/test-id.parquet"
    ).read()
    assert pd.read_parquet(BytesIO(saved_content)).equals(content)


@mark.integration
def test_load_df_parquet():
    data_repository = MinioRepository(
        MINIO_ENDPOINT, MINIO_BUCKET, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False
    )
    loaded_content = data_repository.load(
        "test-project", "raw", "test-id", "parquet", ContentType.DF
    )
    expected_content = pd.DataFrame({"a": [1], "b": [2]})
    assert loaded_content.equals(expected_content)
