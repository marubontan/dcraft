from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, List, Optional

import pandas as pd

from dcraft.domain.error import NotCoveredContentType, NotCoveredFormat
from dcraft.domain.type.enum import ContentType
from dcraft.interface.data.base import DataRepository


@dataclass
class Metadata:
    id: str
    project_name: str
    layer: str
    content_type: ContentType
    author: Optional[str]
    created_at: datetime
    description: Optional[str]
    extra_info: Optional[dict]
    source_ids: Optional[List[str]]
    format: str

    @property
    def asdict(self):
        return {
            "id": self.id,
            "project_name": self.project_name,
            "layer": self.layer,
            "content_type": self.content_type.name,
            "author": self.author,
            "created_at": self.created_at,
            "description": self.description,
            "extra_info": self.extra_info,
            "source_ids": self.source_ids,
            "format": self.format,
        }


class BaseLayerData(ABC):
    def __init__(
        self,
        id: Optional[str],
        project_name: str,
        content: Any,
        author: Optional[str],
        created_at: datetime,
        description: Optional[str],
        extra_info: Optional[dict],
    ):
        self._id = id
        self._project_name = project_name
        self._content = content
        self._content_type = self._get_content_type(content)
        self._author = author
        self._created_at = created_at
        self._description = description
        self._extra_info = extra_info

    @property
    def content(self) -> Any:
        return self._content

    @property
    def id(self) -> Optional[str]:
        return self._id

    @abstractmethod
    def save(self, content: Any, format: str, data_repository: DataRepository):
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
    def _compose_metadata(self, format: str) -> Metadata:
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
