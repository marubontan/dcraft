from typing import Union

from dcraft.domain.layer.raw import RawLayerData
from dcraft.domain.layer.refined import RefinedLayerData
from dcraft.domain.layer.trusted import TrustedLayerData
from dcraft.interface.data.base import DataRepository
from dcraft.interface.metadata.base import MetadataRepository


def read_layer_data(
    id: str, data_repository: DataRepository, metadata_repository: MetadataRepository
) -> Union[RawLayerData, TrustedLayerData, RefinedLayerData]:
    metadata = metadata_repository.load(id)
    content = data_repository.load(
        metadata.project_name,
        metadata.layer,
        id,
        metadata.format,
        metadata.content_type,
    )
    layer_data: Union[RawLayerData, TrustedLayerData, RefinedLayerData]
    if metadata.layer == "raw":
        layer_data = RawLayerData(
            id,
            metadata.project_name,
            content,
            metadata.author,
            metadata.created_at,
            metadata.description,
            metadata.extra_info,
        )
    elif metadata.layer == "trusted":
        layer_data = TrustedLayerData(
            id,
            metadata.project_name,
            content,
            metadata.author,
            metadata.created_at,
            metadata.description,
            metadata.extra_info,
            metadata.source_ids,
        )
    else:
        layer_data = RefinedLayerData(
            id,
            metadata.project_name,
            content,
            metadata.author,
            metadata.created_at,
            metadata.description,
            metadata.extra_info,
            metadata.source_ids,
        )
    return layer_data
