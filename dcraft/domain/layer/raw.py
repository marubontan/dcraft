from datetime import datetime
from typing import Optional
from uuid import uuid4

from dcraft.domain.layer.base import BaseLayerData
from dcraft.domain.metadata import Metadata
from dcraft.domain.type.content import CoveredContentType
from dcraft.interface.data.base import DataRepository
from dcraft.interface.metadata.base import MetadataRepository


class RawLayerData(BaseLayerData):
    """This stores raw data and manage.

    This class is used to store raw data.
    Loaded raw layer data is stored on this class. And also, this manages saving and metadata.

    Attributes:
        id (str, optional): Unique id for the data and metadata
        project_name (str): Name of the project
        content (CoveredContentType): Content of the data
        author (str, optional): Author of the data
        created_at (datetime): Created at
        description (str, optional): Description of the data
        extra_info (dict, optional): Extra information of the data

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

    def _compose_metadata(self, id: str, format: str) -> Metadata:
        return Metadata(
            id=id,
            project_name=self._project_name,
            layer="raw",
            content_type=self._get_content_type(self._content),
            author=self._author,
            created_at=self._created_at,
            description=self._description,
            extra_info=self._extra_info,
            source_ids=None,
            format=format,
        )

    def save(
        self,
        format: str,
        data_repository: DataRepository,
        metadata_repository: MetadataRepository,
    ):
        """Saves the content of the object to the data repository and the metadata to the metadata repository.
        On the timing of saving, the id of the object will be updated.

        Args:
            format (str): The format in which the content will be saved.
            data_repository (DataRepository): The data repository where the content will be saved.
            metadata_repository (MetadataRepository): The metadata repository where the metadata will be saved.

        Returns:
            None
        """
        self._validate_format(self._content, format)
        id = self._generate_id()
        self._update_id(id)
        data_repository.save(
            self.content,
            self._project_name,
            "raw",
            id,
            format,
            self._get_content_type(self._content),
        )
        metadata = self._compose_metadata(id, format)
        metadata_repository.save(metadata)
