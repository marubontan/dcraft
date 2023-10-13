from itertools import product

import pandas as pd
from pytest import mark

from dcraft import (
    LocalDataRepository,
    LocalMetadataRepository,
    create_raw,
    create_refined,
    create_trusted,
    read_layer_data,
)

try:
    from dcraft import BqMetadataRepository, GcsDataRepository

    WITH_BQ = True

except ImportError:
    WITH_BQ = False

try:
    from dcraft import *

    WITH_MONGO = True
except ImportError:
    WITH_MONGO = False
try:
    from dcraft import GcsDataRepository

    WITH_GCS = True
except ImportError:
    WITH_GCS = False

from .setting import (
    BQ_DATASET_ID,
    BQ_TABLE_ID,
    GCP_PROJECT,
    GCS_BUCKET,
    MONGO_COLLECTION,
    MONGO_DB,
    MONGO_HOST,
    MONGO_PORT,
)


def compose_parameters():
    DATA_REPOSITORIES = [LocalDataRepository]
    if WITH_GCS:
        DATA_REPOSITORIES.append(GcsDataRepository)
    METADATA_REPOSITORIES = [LocalMetadataRepository]
    if WITH_BQ:
        METADATA_REPOSITORIES.append(BqMetadataRepository)
    if WITH_MONGO:
        METADATA_REPOSITORIES.append(MongoMetadataRepository)
    DICT_CONTENTS = [{"a": 1, "b": 2}, [{"a": 1, "b": 2}, {"a": 3, "b": 4}]]
    DICT_FORMATS = ["json"]
    DF_CONTENTS = [pd.DataFrame({"a": [1], "b": [2]})]
    DF_FORMATS = ["csv", "parquet"]

    data_and_metadata = list(product(DATA_REPOSITORIES, METADATA_REPOSITORIES))
    dict_content_and_format = product(DICT_CONTENTS, DICT_FORMATS)
    df_content_and_format = product(DF_CONTENTS, DF_FORMATS)
    return [
        (*p[0], *p[1], lambda x: x, lambda x: x)
        for p in product(data_and_metadata, dict_content_and_format)
    ] + [
        (*p[0], *p[1], lambda x: x, lambda x: x)
        for p in product(data_and_metadata, df_content_and_format)
    ]


@mark.e2e
@mark.parametrize(
    "data_repository_class, metadata_repository_class, content, format, raw_to_trusted_processor, trusted_to_refined_processor",
    compose_parameters(),
)
def test_general_usecase(
    data_repository_class,
    metadata_repository_class,
    content,
    format,
    raw_to_trusted_processor,
    trusted_to_refined_processor,
    tmp_path,
):
    # Define reqpositories
    if data_repository_class is LocalDataRepository:
        data_repository = data_repository_class(tmp_path)
    else:
        data_repository = data_repository_class(GCP_PROJECT, GCS_BUCKET)
    if metadata_repository_class is LocalMetadataRepository:
        metadata_repository = metadata_repository_class(tmp_path)
    elif metadata_repository_class is BqMetadataRepository:
        metadata_repository = metadata_repository_class(
            GCP_PROJECT, BQ_DATASET_ID, BQ_TABLE_ID
        )
    else:
        metadata_repository = metadata_repository_class(
            MONGO_HOST, MONGO_PORT, MONGO_DB, MONGO_COLLECTION
        )

    # Create layers data
    raw_layer_data = create_raw(content, "test-project")
    trusted_layer_data = create_trusted(
        raw_to_trusted_processor(raw_layer_data.content), "test-project"
    )
    refined_layer_data = create_refined(
        trusted_to_refined_processor(trusted_layer_data.content), "test-project"
    )

    # Save layers data and metadata
    raw_layer_data.save(format, data_repository, metadata_repository)
    trusted_layer_data.save(format, data_repository, metadata_repository)
    refined_layer_data.save(format, data_repository, metadata_repository)

    # Load layers data
    loaded_raw_layer_data = read_layer_data(
        raw_layer_data.id, data_repository, metadata_repository
    )
    loaded_trusted_layer_data = read_layer_data(
        trusted_layer_data.id, data_repository, metadata_repository
    )
    loaded_refined_layer_data = read_layer_data(
        refined_layer_data.id, data_repository, metadata_repository
    )

    # Assertion
    if isinstance(raw_layer_data.content, pd.DataFrame):
        assert loaded_raw_layer_data.content.equals(raw_layer_data.content)
    else:
        assert loaded_raw_layer_data.content == raw_layer_data.content

    if isinstance(trusted_layer_data.content, pd.DataFrame):
        assert loaded_trusted_layer_data.content.equals(trusted_layer_data.content)
    else:
        assert loaded_trusted_layer_data.content == trusted_layer_data.content

    if isinstance(refined_layer_data.content, pd.DataFrame):
        assert loaded_refined_layer_data.content.equals(refined_layer_data.content)
    else:
        assert loaded_refined_layer_data.content == refined_layer_data.content
