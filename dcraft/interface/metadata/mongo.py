import json

from pymongo import MongoClient

from dcraft.domain.metadata import Metadata
from dcraft.domain.type.enum import ContentType
from dcraft.interface.metadata.base import MetadataRepository


class MongoMetadataRepository(MetadataRepository):
    def __init__(self, host: str, port: int, db: str, collection: str):
        self._host = host
        self._port = port
        self._db = db
        self._collection = collection
        self._client = MongoClient(host, port)

    def load(self, id: str) -> Metadata:
        """Loads metadata for a specific ID.

        Args:
            id (str): The ID of the metadata to load.

        Returns:
            Metadata: The loaded metadata object.
        """
        collection = self._client[self._db][self._collection]
        cursor = collection.find({"id": id})
        for document in cursor:
            metadata = Metadata(
                id=document["id"],
                project_name=document["project_name"],
                layer=document["layer"],
                content_type=ContentType[document["content_type"]],
                author=document["author"],
                created_at=document["created_at"],
                description=document["description"],
                extra_info=json.loads(document["extra_info"])
                if document["extra_info"] is not None
                else None,
                source_ids=document["source_ids"],
                format=document["format"],
            )
            break
        return metadata

    def save(self, metadata: Metadata):
        """Save the given metadata to the database.

        Args:
            metadata (Metadata): The metadata object to save.

        Returns:
            None
        """
        metadata_dict = metadata.asdict
        metadata_dict["extra_info"] = (
            json.dumps(metadata_dict["extra_info"])
            if metadata_dict["extra_info"] is not None
            else None
        )
        collection = self._client[self._db][self._collection]
        collection.insert_one(metadata_dict)
