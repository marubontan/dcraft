import json
import os
from datetime import datetime

from dcraft.domain.error import NoMetadataFound
from dcraft.domain.metadata import Metadata
from dcraft.domain.type.enum import ContentType
from dcraft.interface.metadata.base import MetadataRepository
from dcraft.interface.metadata.setting import LOCAL_METADATA_NAME


class LocalMetadataRepository(MetadataRepository):
    def __init__(self, path):
        self._path = path
        self._metadata_path = self._compose_path()

    def load(self, id: str) -> Metadata:
        """Load the metadata for a given ID.

        Args:
            id (str): The ID of the metadata to load.

        Returns:
            Metadata: The loaded metadata.

        Raises:
            NoMetadataFound: If no metadata is found for the given ID.
        """
        with open(self._metadata_path, "r") as f:
            for line in f:
                metadata_dict = json.loads(line)
                if metadata_dict["id"] == id:
                    if metadata_dict["created_at"] is not None:
                        metadata_dict["created_at"] = datetime.fromisoformat(
                            metadata_dict["created_at"]
                        )

                    return Metadata(
                        id=metadata_dict["id"],
                        project_name=metadata_dict["project_name"],
                        layer=metadata_dict["layer"],
                        content_type=ContentType[metadata_dict["content_type"]],
                        author=metadata_dict.get("author"),
                        created_at=metadata_dict["created_at"],
                        description=metadata_dict.get("description"),
                        extra_info=metadata_dict.get("extra_info"),
                        source_ids=metadata_dict.get("source_ids"),
                        format=metadata_dict["format"],
                    )
            else:
                raise NoMetadataFound(f"No Metadata found for {id}")

    def save(self, metadata: Metadata):
        """Saves the given metadata to a file.

        Parameters:
            metadata (Metadata): The metadata object to be saved.

        Returns:
            None
        """
        metadata_dict = metadata.asdict
        if metadata_dict["created_at"] is not None:
            metadata_dict["created_at"] = metadata_dict["created_at"].isoformat()
        with open(self._metadata_path, "a") as f:
            f.write(json.dumps(metadata_dict) + "\n")

    def _compose_path(self) -> str:
        return os.path.join(self._path, LOCAL_METADATA_NAME)
