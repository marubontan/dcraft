from abc import ABC, abstractmethod

from dcraft.domain.type.content import CoveredContentType
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
    ) -> CoveredContentType:
        """Load the specified project, layer, and content based on the given parameters.

        Args:
            project_name (str): The name of the project to load.
            layer_name (str): The name of the layer to load.
            id (str): The ID of the content to load.
            format (str): The format of the content to load.
            content_type (ContentType): The type of the content to load.

        Returns:
            CoveredContentType: The loaded content of the specified project, layer, and ID.
        """
        pass

    @abstractmethod
    def save(
        self,
        content: CoveredContentType,
        project_name: str,
        layer_name: str,
        id: str,
        format: str,
        content_type: ContentType,
    ):
        """Save the given content to a specified location.

        Args:
            content (CoveredContentType): The content to be saved.
            project_name (str): The name of the project.
            layer_name (str): The name of the layer.
            id (str): The ID of the content.
            format (str): The format of the content.
            content_type (ContentType): The type of the content.

        Returns:
            None
        """
        pass

    @abstractmethod
    def _compose_path(
        self, project_name: str, layer_name: str, id: str, format: str
    ) -> str:
        pass
