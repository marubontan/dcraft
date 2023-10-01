from datetime import datetime

from pytest import mark

from dcraft.domain.layer.base import Metadata
from dcraft.domain.type.enum import ContentType
from dcraft.interface.metadata.bq import BqMetadataRepository

from ...setting import GCP_PROJECT


@mark.integration
def test_init():
    metadata_repository = BqMetadataRepository(
        GCP_PROJECT, "test_dataset", "test_table"
    )
    assert metadata_repository._project == GCP_PROJECT
    assert metadata_repository._dataset_id == "test_dataset"
    assert metadata_repository._table_id == "test_table"


@mark.integration
def test_save():
    metadata = Metadata(
        id="test-id",
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
def test_load():
    metadata_repository = BqMetadataRepository(
        GCP_PROJECT, "test_dataset", "test_table"
    )
    metadata = metadata_repository.load("test-id")
    assert metadata == Metadata(
        id="test-id",
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
