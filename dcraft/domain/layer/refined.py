from datetime import datetime
from typing import List, Optional
from uuid import uuid4

from dcraft.domain.layer.base import BaseLayerData
from dcraft.domain.metadata import Metadata
from dcraft.domain.type.content import CoveredContentType
from dcraft.interface.data.base import DataRepository
from dcraft.interface.metadata.base import MetadataRepository


class RefinedLayerData(BaseLayerData):
    """This stores refined data and manage.

    This class is used to store refined data.
    Loaded refined layer data is stored on this class. And also, this manages saving and metadata.

    Attributes:
        id (str, optional): Unique id for the data and metadata
        project_name (str): Name of the project
        content (CoveredContentType): Content of the data
        author (str, optional): Author of the data
        created_at (datetime): Created at
        description (str, optional): Description of the data
        extra_info (dict, optional): Extra information of the data
        source_ids (List[str], optional): List of source ids

    """

    def __init__(
        self,
        id: Optional[str],
        project_name: str,
        content: CoveredContentType,
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

    def _compose_metadata(self, id: str, format: str) -> Metadata:
        return Metadata(
            id=id,
            project_name=self._project_name,
            layer="refined",
            content_type=self._get_content_type(self._content),
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
        """Saves the content of this object to the data repository and metadata repository.
        On the timing of saving, the id of the object will be updated.

        Args:
            format (str): The format in which to save the content.
            data_repository (DataRepository): The data repository used for saving the content.
            metadata_repository (MetadataRepository): The metadata repository used for saving the metadata.

        Returns:
            None
        """

        self._validate_format(self._content, format)
        id = self._generate_id()
        self._update_id(id)
        data_repository.save(
            self.content,
            self._project_name,
            "refined",
            id,
            format,
            self._get_content_type(self._content),
        )
        metadata = self._compose_metadata(id, format)
        metadata_repository.save(metadata)
