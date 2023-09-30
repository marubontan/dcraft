import json
from io import BytesIO, StringIO
from typing import Any

import pandas as pd
from google.cloud.storage import Client

from dcraft.domain.error import ContentExtensionMismatch, NotCoveredContentType
from dcraft.domain.type.enum import ContentType
from dcraft.interface.data.base import DataRepository


class GcsDataRepository(DataRepository):
    def __init__(self, project_id: str, bucket_name: str):
        self._bucket_name = bucket_name
        self._client = Client(project=project_id)
        self._bucket = self._client.get_bucket(self._bucket_name)

    def load(
        self,
        project_name: str,
        layer_name: str,
        id: str,
        format: str,
        content_type: ContentType,
    ) -> Any:
        path = self._compose_path(project_name, layer_name, id, format)
        if content_type == ContentType.DF:
            if format == "csv":
                blob = self._bucket.blob(path)
                csv_string = StringIO(blob.download_as_text())
                data = pd.read_csv(csv_string)
            elif format == "parquet":
                blob = self._bucket.blob(path)
                parquet_bytes = BytesIO(blob.download_as_bytes())
                data = pd.read_parquet(parquet_bytes)
            else:
                raise ContentExtensionMismatch(
                    "This content can't be saved with this extension."
                )
        elif content_type == ContentType.DICT:
            if format == "json":
                blob = self._bucket.blob(path)
                dict_bytes = blob.download_as_text()
                data = json.loads(dict_bytes)
            else:
                raise ContentExtensionMismatch(
                    "This content can't be saved with this extension."
                )
        else:
            raise NotCoveredContentType("This content type is not covered.")
        return data

    def save(
        self,
        content: Any,
        project_name: str,
        layer_name: str,
        id: str,
        format: str,
        content_type: ContentType,
    ):
        path = self._compose_path(project_name, layer_name, id, format)
        blob = self._bucket.blob(path)
        if content_type == ContentType.DF:
            if format == "csv":
                data = content.to_csv(index=False)
                blob.upload_from_string(data)
            elif format == "parquet":
                buffer = BytesIO()
                content.to_parquet(buffer, index=False)
                buffer.seek(0)
                blob.upload_from_file(buffer)
                buffer.close()
            else:
                raise ContentExtensionMismatch(
                    "This content can't be saved with this extension."
                )
        elif content_type == ContentType.DICT:
            if format == "json":
                data = json.dumps(content)
                blob.upload_from_string(data)
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
