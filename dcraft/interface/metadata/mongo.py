import json
from typing import Any, Optional, Sequence, Union

from bson.codec_options import TypeRegistry
from pymongo import MongoClient

from dcraft.domain.metadata import Metadata
from dcraft.domain.type.enum import ContentType
from dcraft.interface.metadata.base import MetadataRepository


class MongoMetadataRepository(MetadataRepository):
    def __init__(
        self,
        db: str,
        collection: str,
        host: Optional[Union[str, Sequence[str]]] = None,
        port: Optional[int] = None,
        document_class: Optional[Any] = None,
        tz_aware: Optional[bool] = None,
        connect: Optional[bool] = None,
        type_registry: Optional[TypeRegistry] = None,
    ):
        """Initializes a new instance of the class.

        Args:
            db (str): The name of the database.
            collection (str): The name of the collection.
            host (Optional[Union[str, Sequence[str]]], optional): The host(s) to connect to. Defaults to None.
            port (Optional[int], optional): The port number. Defaults to None.
            document_class (Optional[Any], optional): The default class to use for documents. Defaults to None.
            tz_aware (Optional[bool], optional): Whether to be timezone aware. Defaults to None.
            connect (Optional[bool], optional): Whether to connect on initialization. Defaults to None.
            type_registry (Optional[TypeRegistry], optional): The type registry. Defaults to None.
        """
        self._host = host
        self._port = port
        self._db = db
        self._collection = collection
        self._client = MongoClient(
            host=host,
            port=port,
            document_class=document_class,
            tz_aware=tz_aware,
            connect=connect,
            type_registry=type_registry,
        )

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
