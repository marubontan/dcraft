from datetime import datetime
from typing import Any, List, Optional

from dcraft.domain.layer.base import BaseLayerData, Metadata
from dcraft.interface.data.base import DataRepository
from dcraft.interface.metadata.base import MetadataRepository


class RefinedLayerData(BaseLayerData):
    def __init__(
        self,
        id: str,
        project_name: str,
        content: Any,
        author: Optional[str],
        created_at: datetime,
        description: Optional[str],
        extra_info: Optional[dict],
        source_ids: Optional[List[str]],
    ):
        super().__init__(
            id=id,
            project_name=project_name,
            content=content,
            author=author,
            created_at=created_at,
            description=description,
            extra_info=extra_info,
        )
        self._source_ids = source_ids

    def _compose_metadata(self, format: str) -> Metadata:
        return Metadata(
            id=self._id,
            project_name=self._project_name,
            layer="refined",
            content_type=self._content_type,
            author=self._author,
            created_at=self._created_at,
            description=self._description,
            extra_info=self._extra_info,
            source_ids=self._source_ids,
            format=format,
        )

    def save(
        self,
        format: str,
        data_repository: DataRepository,
        metadata_repository: MetadataRepository,
    ):
        self._validate_format(self.content, format)
        data_repository.save(
            self.content,
            self._project_name,
            "refined",
            self._id,
            format,
            self._content_type,
        )
        metadata = self._compose_metadata(format)
        metadata_repository.save(metadata)
