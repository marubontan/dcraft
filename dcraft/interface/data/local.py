import json
import os

import pandas as pd

from dcraft.domain.error import ContentExtensionMismatch, NotCoveredContentType
from dcraft.domain.type.content import CoveredContentType
from dcraft.domain.type.enum import ContentType
from dcraft.interface.data.base import DataRepository


class LocalDataRepository(DataRepository):
    def __init__(self, dir_path: str):
        self._dir_path = dir_path

    def load(
        self,
        project_name: str,
        layer_name: str,
        id: str,
        format: str,
        content_type: ContentType,
    ) -> CoveredContentType:
        """Load the content from a specified path based on the project name, layer name, id, format, and content type.

        Args:
            project_name (str): The name of the project.
            layer_name (str): The name of the layer.
            id (str): The ID of the content.
            format (str): The format of the content.
            content_type (ContentType): The type of the content.

        Returns:
            CoveredContentType: The loaded content.

        Raises:
            ContentExtensionMismatch: If the content cannot be saved with the specified extension.
            NotCoveredContentType: If the content type is not covered.
        """
        path = self._compose_path(project_name, layer_name, id, format)
        if content_type == ContentType.DF:
            if format == "csv":
                data = pd.read_csv(path)
            elif format == "parquet":
                data = pd.read_parquet(path)
            else:
                raise ContentExtensionMismatch(
                    "This content can't be saved with this extension."
                )
        elif content_type == ContentType.DICT:
            if format == "json":
                with open(path, "r") as f:
                    data = json.load(f)
            else:
                raise ContentExtensionMismatch(
                    "This content can't be saved with this extension."
                )
        else:
            raise NotCoveredContentType("This content type is not covered.")
        return data

    def save(
        self,
        content: CoveredContentType,
        project_name: str,
        layer_name: str,
        id: str,
        format: str,
        content_type: ContentType,
    ):
        """Saves the given content to a file with the specified project name, layer name, ID, format, and content type.

        Args:
            content (CoveredContentType): The content to be saved.
            project_name (str): The name of the project.
            layer_name (str): The name of the layer.
            id (str): The ID of the content.
            format (str): The format of the file to be saved.
            content_type (ContentType): The type of the content.

        Raises:
            ContentExtensionMismatch: If the content can't be saved with the specified extension.
            NotCoveredContentType: If the content type is not covered.

        Returns:
            None
        """
        path = self._compose_path(project_name, layer_name, id, format)
        self._mkdirs(path)
        if content_type == ContentType.DF:
            if format == "csv":
                content.to_csv(path, index=False)
            elif format == "parquet":
                content.to_parquet(path, index=False)
            else:
                raise ContentExtensionMismatch(
                    "This content can't be saved with this extension."
                )
        elif content_type in [ContentType.DICT, ContentType.DICT_LIST]:
            if format == "json":
                with open(path, "w") as f:
                    json.dump(content, f)
            else:
                raise ContentExtensionMismatch(
                    "This content can't be saved with this extension."
                )
        else:
            raise NotCoveredContentType("This content type is not covered.")

    def _mkdirs(self, path: str):
        os.makedirs(os.path.dirname(path), exist_ok=True)

    def _compose_path(
        self, project_name: str, layer_name: str, id: str, format: str
    ) -> str:
        file_path = (
            f"{os.path.join(self._dir_path, project_name, layer_name, id)}.{format}"
        )
        return file_path
