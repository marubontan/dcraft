from abc import ABC, abstractmethod

from dcraft.domain.metadata import Metadata


class MetadataRepository(ABC):
    @abstractmethod
    def load(self, id: str) -> Metadata:
        """Load the metadata for a specific ID.

        Args:
            id (str): The ID of the metadata to load.

        Returns:
            Metadata: The loaded metadata object.
        """
        pass

    @abstractmethod
    def save(self, metadata: Metadata):
        """Save the metadata.

        Args:
            metadata (Metadata): The metadata object to be saved.

        Returns:
            None
        """
        pass
