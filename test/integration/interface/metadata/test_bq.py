from datetime import datetime
from uuid import uuid4

from pytest import fixture, mark

from dcraft.domain.metadata import Metadata
from dcraft.domain.type.enum import ContentType

try:
    from dcraft.interface.metadata.bq import BqMetadataRepository
except ImportError:
    pass

from ...setting import GCP_PROJECT


@fixture(scope="module")
def id():
    return str(uuid4())


@mark.integration
def test_init():
    metadata_repository = BqMetadataRepository(
        GCP_PROJECT, "test_dataset", "test_table"
    )
    assert metadata_repository._project == GCP_PROJECT
    assert metadata_repository._dataset_id == "test_dataset"
    assert metadata_repository._table_id == "test_table"


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

    metadata_repository = BqMetadataRepository(
        GCP_PROJECT, "test_dataset", "test_table"
    )

    metadata_repository.save(metadata)


@mark.integration
def test_load(id):
    metadata_repository = BqMetadataRepository(
        GCP_PROJECT, "test_dataset", "test_table"
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
        source_ids=[],
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

    metadata_repository = BqMetadataRepository(
        GCP_PROJECT, "test_dataset", "test_table"
    )

    metadata_repository.save(metadata)
