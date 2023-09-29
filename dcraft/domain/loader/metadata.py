from dcraft.domain.layer.base import Metadata
from dcraft.interface.metadata.base import MetadataRepository


def read_metadata(id: str, metadata_repository: MetadataRepository) -> Metadata:
    return metadata_repository.load(id)
