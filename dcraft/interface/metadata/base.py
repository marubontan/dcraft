from abc import ABC, abstractmethod

from dcraft.domain.layer.base import Metadata


class MetadataRepository(ABC):
    @abstractmethod
    def load(self, id: str) -> Metadata:
        pass

    @abstractmethod
    def save(self, metadata: Metadata):
        pass
