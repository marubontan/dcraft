from abc import ABC, abstractmethod
from typing import Any

from dcraft.domain.type.enum import ContentType


class DataRepository(ABC):
    def __init__(self, path: str):
        self._path = path

    @abstractmethod
    def load(
        self,
        project_name: str,
        layer_name: str,
        id: str,
        content_type: ContentType,
        format: str,
    ) -> Any:
        pass

    @abstractmethod
    def save(
        self,
        content: Any,
        project_name: str,
        layer_name: str,
        id: str,
        format: str,
        content_type: ContentType,
    ):
        pass

    @abstractmethod
    def _compose_path(
        self, project_name: str, layer_name: str, id: str, format: str
    ) -> str:
        pass
