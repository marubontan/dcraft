from dcraft.domain.layer.refined import RefinedLayerData
from dcraft.domain.loader.refined import create_refined


def test_create_refined():
    content = {"a": 1, "b": 2}
    raw_layer_data = create_refined(content, "test-project")

    assert isinstance(raw_layer_data, RefinedLayerData)
