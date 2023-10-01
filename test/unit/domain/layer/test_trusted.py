import json
import os
from datetime import datetime

import pandas as pd

from dcraft.domain.layer.trusted import TrustedLayerData
from dcraft.domain.loader import read_layer_data
from dcraft.interface.data.local import LocalDataRepository
from dcraft.interface.metadata.local import LocalMetadataRepository

from ...setting import SCOPE_PATH


def test_trusted_layer_dict_data_saving(tmp_path):
    content = {"a": 1, "b": 2}
    trusted_data_layer = TrustedLayerData(
        None, "test-project", content, None, datetime.now(), None, None, None
    )

    data_repository = LocalDataRepository(tmp_path)
    metadata_repository = LocalMetadataRepository(tmp_path)
    trusted_data_layer.save("json", data_repository, metadata_repository)

    with open(
        os.path.join(
            tmp_path, "test-project", "trusted", f"{trusted_data_layer.id}.json"
        )
    ) as f:
        saved_content = json.load(f)
        assert saved_content == content

    with open(os.path.join(tmp_path, "metadata.jsonl")) as f:
        saved_metadata = json.load(f)
        saved_metadata["created_at"] = datetime.fromisoformat(
            saved_metadata["created_at"]
        )
        assert saved_metadata == trusted_data_layer._compose_metadata("json").asdict


def test_trusted_layer_pandas_data_saving(tmp_path):
    content = pd.DataFrame({"a": [1], "b": [2]})
    trusted_data_layer = TrustedLayerData(
        None, "test-project", content, None, datetime.now(), None, None, None
    )

    data_repository = LocalDataRepository(tmp_path)
    metadata_repository = LocalMetadataRepository(tmp_path)
    trusted_data_layer.save("csv", data_repository, metadata_repository)

    saved_content = pd.read_csv(
        os.path.join(
            tmp_path, "test-project", "trusted", f"{trusted_data_layer.id}.csv"
        )
    )
    assert saved_content.equals(content)

    with open(os.path.join(tmp_path, "metadata.jsonl")) as f:
        saved_metadata = json.load(f)
        saved_metadata["created_at"] = datetime.fromisoformat(
            saved_metadata["created_at"]
        )
        assert saved_metadata == trusted_data_layer._compose_metadata("csv").asdict


def test_trusted_layer_load():
    data_repository = LocalDataRepository(SCOPE_PATH)
    metadata_repository = LocalMetadataRepository(SCOPE_PATH)
    trusted_layer_data = read_layer_data(
        "test-id-2", data_repository, metadata_repository
    )

    assert isinstance(trusted_layer_data, TrustedLayerData)

    expected_content = {"a": 0, "b": 2}
    assert trusted_layer_data.content == expected_content
