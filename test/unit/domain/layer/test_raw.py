import json
import os
from datetime import datetime

import pandas as pd
import pytest

from dcraft.domain.error import NotCoveredContentType
from dcraft.domain.layer.raw import RawLayerData
from dcraft.domain.loader import read_layer_data
from dcraft.interface.data.local import LocalDataRepository
from dcraft.interface.metadata.local import LocalMetadataRepository

from ...setting import SCOPE_PATH


def test_raw_layer_data_init():
    content = {"a": 1, "b": 2}
    RawLayerData(None, "test-project", content, None, datetime.now(), None, None)


def test_raw_layer_data_init_not_covered_content():
    content = "test"
    with pytest.raises(NotCoveredContentType):
        RawLayerData(None, "test-project", content, None, datetime.now(), None, None)


def test_raw_layer_dict_data_saving(tmp_path):
    content = {"a": 1, "b": 2}
    raw_data_layer = RawLayerData(
        None, "test-project", content, None, datetime.now(), None, None
    )

    data_repository = LocalDataRepository(tmp_path)
    metadata_repository = LocalMetadataRepository(tmp_path)
    raw_data_layer.save("json", data_repository, metadata_repository)

    with open(
        os.path.join(tmp_path, "test-project", "raw", f"{raw_data_layer.id}.json")
    ) as f:
        saved_content = json.load(f)
        assert saved_content == content

    with open(os.path.join(tmp_path, "metadata.jsonl")) as f:
        saved_metadata = json.load(f)
        saved_metadata["created_at"] = datetime.fromisoformat(
            saved_metadata["created_at"]
        )
        assert (
            saved_metadata
            == raw_data_layer._compose_metadata(raw_data_layer._id, "json").asdict
        )


def test_raw_layer_dict_list_data_saving(tmp_path):
    content = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    raw_data_layer = RawLayerData(
        None, "test-project", content, None, datetime.now(), None, None
    )

    data_repository = LocalDataRepository(tmp_path)
    metadata_repository = LocalMetadataRepository(tmp_path)
    raw_data_layer.save("json", data_repository, metadata_repository)

    with open(
        os.path.join(tmp_path, "test-project", "raw", f"{raw_data_layer.id}.json")
    ) as f:
        saved_content = json.load(f)
        assert saved_content == content

    with open(os.path.join(tmp_path, "metadata.jsonl")) as f:
        saved_metadata = json.load(f)
        saved_metadata["created_at"] = datetime.fromisoformat(
            saved_metadata["created_at"]
        )
        assert (
            saved_metadata
            == raw_data_layer._compose_metadata(raw_data_layer._id, "json").asdict
        )


def test_raw_layer_pandas_data_saving(tmp_path):
    content = pd.DataFrame({"a": [1], "b": [2]})
    raw_data_layer = RawLayerData(
        None, "test-project", content, None, datetime.now(), None, None
    )

    data_repository = LocalDataRepository(tmp_path)
    metadata_repository = LocalMetadataRepository(tmp_path)
    raw_data_layer.save("csv", data_repository, metadata_repository)

    saved_content = pd.read_csv(
        os.path.join(tmp_path, "test-project", "raw", f"{raw_data_layer.id}.csv")
    )
    assert saved_content.equals(content)

    with open(os.path.join(tmp_path, "metadata.jsonl")) as f:
        saved_metadata = json.load(f)
        saved_metadata["created_at"] = datetime.fromisoformat(
            saved_metadata["created_at"]
        )
        assert (
            saved_metadata
            == raw_data_layer._compose_metadata(raw_data_layer._id, "csv").asdict
        )


def test_raw_layer_load():
    data_repository = LocalDataRepository(SCOPE_PATH)
    metadata_repository = LocalMetadataRepository(SCOPE_PATH)
    raw_layer_data = read_layer_data("test-id-1", data_repository, metadata_repository)

    assert isinstance(raw_layer_data, RawLayerData)

    expected_content = {"a": 1, "b": 2}
    assert raw_layer_data.content == expected_content
