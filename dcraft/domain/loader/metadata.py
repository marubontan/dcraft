from dcraft.domain.metadata import Metadata
from dcraft.interface.metadata.base import MetadataRepository


def read_metadata(id: str, metadata_repository: MetadataRepository) -> Metadata:
    """
    Retrieves metadata for a given ID.

    Args:
        id (str): The ID of the metadata to retrieve.
        metadata_repository (MetadataRepository): The repository to load the metadata from.

    Returns:
        Metadata: The loaded metadata.
    """
    return metadata_repository.load(id)
