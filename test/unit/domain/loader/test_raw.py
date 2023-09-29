from dcraft.domain.layer.raw import RawLayerData
from dcraft.domain.loader.raw import create_raw


def test_create_raw():
    content = {"a": 1, "b": 2}
    raw_layer_data = create_raw(content, "test-project")

    assert isinstance(raw_layer_data, RawLayerData)
