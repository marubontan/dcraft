from abc import ABC, abstractmethod

from dcraft.domain.layer.base import Metadata


class MetadataRepository(ABC):
    def __init__(self, path: str):
        self._path = path

    @abstractmethod
    def load(self, id: str) -> Metadata:
        pass

    @abstractmethod
    def save(self, metadata: Metadata):
        pass
