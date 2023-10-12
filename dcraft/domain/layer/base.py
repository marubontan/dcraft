from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Optional
from uuid import uuid4

import pandas as pd

from dcraft.domain.error import NotCoveredContentType, NotCoveredFormat
from dcraft.domain.metadata import Metadata
from dcraft.domain.type.content import CoveredContentType
from dcraft.domain.type.enum import ContentType
from dcraft.interface.data.base import DataRepository
from dcraft.interface.metadata.base import MetadataRepository


class BaseLayerData(ABC):
    def __init__(
        self,
        id: Optional[str],
        project_name: str,
        content: CoveredContentType,
        author: Optional[str],
        created_at: datetime,
        description: Optional[str],
        extra_info: Optional[dict],
    ):
        self._get_content_type(content)
        self._id = id
        self._project_name = project_name
        self._content = content
        self._author = author
        self._created_at = created_at
        self._description = description
        self._extra_info = extra_info

    @property
    def content(self) -> Any:
        return self._content

    @property
    def content_type(self) -> ContentType:
        return self._get_content_type(self._content)

    @property
    def id(self) -> Optional[str]:
        return self._id

    @abstractmethod
    def save(
        self,
        format: str,
        data_repository: DataRepository,
        metadata_repository: MetadataRepository,
    ):
        """Save the content in the specified format using the provided data repository.

        Args:
            format (str): The format in which the content will be saved.
            data_repository (DataRepository): The data repository where the content will be saved.
            metadata_repository (MetadataRepository): The metadata repository where the metadata will be saved.

        Returns:
            None
        """
        pass

    def _validate_format(self, content: Any, format: str):
        content_type = self._get_content_type(content)
        if content_type == ContentType.DF:
            if format not in ["csv", "parquet"]:
                raise NotCoveredFormat("This format is not covered.")
        elif content_type in [ContentType.DICT, ContentType.DICT_LIST]:
            if format not in ["json"]:
                raise NotCoveredFormat("This format is not covered.")

    @abstractmethod
    def _compose_metadata(self, id: str, format: str) -> Metadata:
        pass

    @staticmethod
    def _get_content_type(content: Any) -> ContentType:
        if isinstance(content, pd.DataFrame):
            return ContentType.DF
        elif isinstance(content, dict):
            return ContentType.DICT
        elif isinstance(content, list) and all(
            [isinstance(item, dict) for item in content]
        ):
            return ContentType.DICT_LIST
        else:
            raise NotCoveredContentType("This content type is not covered.")

    @staticmethod
    def _generate_id() -> str:
        return str(uuid4())

    def _update_id(self, id: str):
        self._id = id
