from dcraft.domain.layer.trusted import TrustedLayerData
from dcraft.domain.loader.trusted import create_trusted


def test_create_trusted():
    content = {"a": 1, "b": 2}
    raw_layer_data = create_trusted(content, "test-project")

    assert isinstance(raw_layer_data, TrustedLayerData)
