from abc import ABC, abstractmethod
from typing import Any

from dcraft.domain.type.enum import ContentType


class DataRepository(ABC):
    @abstractmethod
    def load(
        self,
        project_name: str,
        layer_name: str,
        id: str,
        format: str,
        content_type: ContentType,
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
