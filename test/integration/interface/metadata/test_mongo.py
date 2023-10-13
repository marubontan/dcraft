from datetime import datetime
from uuid import uuid4

from pymongo import MongoClient
from pytest import fixture, mark

from dcraft.domain.metadata import Metadata
from dcraft.domain.type.enum import ContentType
from dcraft.interface.metadata.mongo import MongoMetadataRepository

from ...setting import MONGO_COLLECTION, MONGO_DB, MONGO_HOST, MONGO_PORT


@fixture(scope="module")
def id():
    return str(uuid4())


@mark.integration
def test_init():
    metadata_repository = MongoMetadataRepository(
        MONGO_HOST, MONGO_PORT, MONGO_DB, MONGO_COLLECTION
    )
    assert metadata_repository._host == MONGO_HOST
    assert metadata_repository._port == MONGO_PORT
    assert metadata_repository._db == MONGO_DB
    assert metadata_repository._collection == MONGO_COLLECTION


@mark.integration
def test_save(id):
    metadata = Metadata(
        id=id,
        project_name="test-project",
        layer="raw",
        content_type=ContentType.DICT,
        author="test-author",
        created_at=datetime(2023, 1, 1),
        description="test-description",
        extra_info=None,
        source_ids=None,
        format="json",
    )

    metadata_repository = MongoMetadataRepository(
        MONGO_HOST, MONGO_PORT, MONGO_DB, MONGO_COLLECTION
    )
    metadata_repository.save(metadata)


@mark.integration
def test_load(id):
    metadata_repository = MongoMetadataRepository(
        MONGO_HOST, MONGO_PORT, MONGO_DB, MONGO_COLLECTION
    )
    metadata = metadata_repository.load(id)
    assert metadata == Metadata(
        id=id,
        project_name="test-project",
        layer="raw",
        content_type=ContentType.DICT,
        author="test-author",
        created_at=datetime(2023, 1, 1),
        description="test-description",
        extra_info=None,
        source_ids=None,
        format="json",
    )


@mark.integration
def test_save_with_optional_information(id):
    metadata = Metadata(
        id=id,
        project_name="test-project",
        layer="raw",
        content_type=ContentType.DICT,
        author="test-author",
        created_at=datetime(2023, 1, 1),
        description="test-description",
        extra_info={"a": 1, "b": 2},
        source_ids=["test-source-1", "test-source-2"],
        format="json",
    )

    metadata_repository = MongoMetadataRepository(
        MONGO_HOST, MONGO_PORT, MONGO_DB, MONGO_COLLECTION
    )

    metadata_repository.save(metadata)
