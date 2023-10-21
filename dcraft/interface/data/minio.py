import json
from io import BytesIO, StringIO

import pandas as pd
from minio import Minio

from dcraft.domain.error import ContentExtensionMismatch, NotCoveredContentType
from dcraft.domain.type.content import CoveredContentType
from dcraft.domain.type.enum import ContentType
from dcraft.interface.data.base import DataRepository


class MinioRepository(DataRepository):
    def __init__(self, endpoint: str, access_key: str, secret_key: str, bucket: str):
        # TODO: Accept parameters which Minio gets
        self._client = Minio(endpoint, access_key, secret_key, secure=False)
        self._bucket = bucket

    def load(
        self,
        project_name: str,
        layer_name: str,
        id: str,
        format: str,
        content_type: ContentType,
    ) -> CoveredContentType:
        """Load the specified content from the given project, layer, and ID.

        Args:
            project_name (str): The name of the project.
            layer_name (str): The name of the layer.
            id (str): The ID of the content.
            format (str): The format of the content.
            content_type (ContentType): The type of the content.

        Returns:
            CoveredContentType: The loaded content.

        Raises:
            ContentExtensionMismatch: If the content cannot be saved with the given extension.
        """
        path = self._compose_path(project_name, layer_name, id, format)
        if content_type == ContentType.DF:
            if format == "csv":
                object = (
                    self._client.get_object(self._bucket, path).read().decode("utf-8")
                )
                data = pd.read_csv(StringIO(object))
            elif format == "parquet":
                object = self._client.get_object(self._bucket, path).read()
                data = pd.read_parquet(BytesIO(object))
            else:
                raise ContentExtensionMismatch(
                    "This content can't be saved with this extension."
                )
        elif content_type in [ContentType.DICT, ContentType.DICT_LIST]:
            if format == "json":
                object = (
                    self._client.get_object(self._bucket, path).read().decode("utf-8")
                )
                data = json.loads(object)
            else:
                raise ContentExtensionMismatch(
                    "This content can't be saved with this extension."
                )
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
        """Save the content to a specified location in the bucket.

        Args:
            content (CoveredContentType): The content to be saved.
            project_name (str): The name of the project.
            layer_name (str): The name of the layer.
            id (str): The unique identifier.
            format (str): The format of the content.
            content_type (ContentType): The type of the content.

        Raises:
            ContentExtensionMismatch: If the content cannot be saved with the given extension.
            NotCoveredContentType: If the content type is not supported.

        """
        path = self._compose_path(project_name, layer_name, id, format)
        if content_type == ContentType.DF:
            if format == "csv":
                data = content.to_csv(index=False).encode("utf-8")
                self._client.put_object(self._bucket, path, BytesIO(data), len(data))

            elif format == "parquet":
                data = content.to_parquet(index=False)
                self._client.put_object(self._bucket, path, BytesIO(data), len(data))
            else:
                raise ContentExtensionMismatch(
                    "This content can't be saved with this extension."
                )
        elif content_type in [ContentType.DICT, ContentType.DICT_LIST]:
            if format == "json":
                data = json.dumps(content).encode("utf-8")
                self._client.put_object(self._bucket, path, BytesIO(data), len(data))
            else:
                raise ContentExtensionMismatch(
                    "This content can't be saved with this extension."
                )
        else:
            raise NotCoveredContentType("This content type is not covered.")

    def _compose_path(
        self, project_name: str, layer_name: str, id: str, format: str
    ) -> str:
        return f"{project_name}/{layer_name}/{id}.{format}"
