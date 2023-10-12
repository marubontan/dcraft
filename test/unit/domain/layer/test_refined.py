import json
import os
from datetime import datetime

import pandas as pd
import pytest

from dcraft.domain.error import NotCoveredContentType
from dcraft.domain.layer.refined import RefinedLayerData
from dcraft.domain.loader import read_layer_data
from dcraft.interface.data.local import LocalDataRepository
from dcraft.interface.metadata.local import LocalMetadataRepository

from ...setting import SCOPE_PATH


def test_refined_layer_data_init():
    content = {"a": 1, "b": 2}
    RefinedLayerData(
        None, "test-project", content, None, datetime.now(), None, None, None
    )


def test_refined_layer_data_init_not_covered_content():
    content = "test"
    with pytest.raises(NotCoveredContentType):
        RefinedLayerData(
            None, "test-project", content, None, datetime.now(), None, None, None
        )


def test_refined_layer_dict_data_saving(tmp_path):
    content = {"a": 1, "b": 2}
    refined_data_layer = RefinedLayerData(
        None, "test-project", content, None, datetime.now(), None, None, None
    )

    data_repository = LocalDataRepository(tmp_path)
    metadata_repository = LocalMetadataRepository(tmp_path)
    refined_data_layer.save("json", data_repository, metadata_repository)
    with open(
        os.path.join(
            tmp_path, "test-project", "refined", f"{refined_data_layer.id}.json"
        )
    ) as f:
        assert json.load(f) == content

    with open(os.path.join(tmp_path, "metadata.jsonl")) as f:
        saved_metadata = json.load(f)
        saved_metadata["created_at"] = datetime.fromisoformat(
            saved_metadata["created_at"]
        )
        assert (
            saved_metadata
            == refined_data_layer._compose_metadata(
                refined_data_layer._id, "json"
            ).asdict
        )


def test_refined_layer_dict_list_data_saving(tmp_path):
    content = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    refined_data_layer = RefinedLayerData(
        None, "test-project", content, None, datetime.now(), None, None, None
    )

    data_repository = LocalDataRepository(tmp_path)
    metadata_repository = LocalMetadataRepository(tmp_path)
    refined_data_layer.save("json", data_repository, metadata_repository)
    with open(
        os.path.join(
            tmp_path, "test-project", "refined", f"{refined_data_layer.id}.json"
        )
    ) as f:
        assert json.load(f) == content

    with open(os.path.join(tmp_path, "metadata.jsonl")) as f:
        saved_metadata = json.load(f)
        saved_metadata["created_at"] = datetime.fromisoformat(
            saved_metadata["created_at"]
        )
        assert (
            saved_metadata
            == refined_data_layer._compose_metadata(
                refined_data_layer._id, "json"
            ).asdict
        )


def test_refined_layer_pandas_data_saving(tmp_path):
    content = pd.DataFrame({"a": [1], "b": [2]})
    refined_data_layer = RefinedLayerData(
        None, "test-project", content, None, datetime.now(), None, None, None
    )

    data_repository = LocalDataRepository(tmp_path)
    metadata_repository = LocalMetadataRepository(tmp_path)
    refined_data_layer.save("csv", data_repository, metadata_repository)

    saved_content = pd.read_csv(
        os.path.join(
            tmp_path, "test-project", "refined", f"{refined_data_layer.id}.csv"
        )
    )
    assert saved_content.equals(content)

    with open(os.path.join(tmp_path, "metadata.jsonl")) as f:
        saved_metadata = json.load(f)
        saved_metadata["created_at"] = datetime.fromisoformat(
            saved_metadata["created_at"]
        )
        assert (
            saved_metadata
            == refined_data_layer._compose_metadata(
                refined_data_layer._id, "csv"
            ).asdict
        )


def test_trusted_layer_load():
    data_repository = LocalDataRepository(SCOPE_PATH)
    metadata_repository = LocalMetadataRepository(SCOPE_PATH)
    refined_layer_data = read_layer_data(
        "test-id-3", data_repository, metadata_repository
    )

    assert isinstance(refined_layer_data, RefinedLayerData)

    expected_content = {"a": 2, "b": 3}
    assert refined_layer_data.content == expected_content
