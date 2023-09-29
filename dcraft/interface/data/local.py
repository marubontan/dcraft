import json
import os
from typing import Any

import pandas as pd

from dcraft.domain.error import ContentExtensionMismatch
from dcraft.domain.type.enum import ContentType, Extension
from dcraft.interface.data.base import DataRepository


class LocalDataRepository(DataRepository):
    def __init__(self, dir_path: str):
        self._dir_path = dir_path

    def load(
        self,
        project_name: str,
        layer_name: str,
        id: str,
        content_type: ContentType,
        format: str,
    ) -> Any:
        path = self._compose_path(project_name, layer_name, id, format)
        if content_type == ContentType.DF:
            if format == "csv":
                data = pd.read_csv(path)
            elif format == "parquet":
                data = pd.read_parquet(path)
            else:
                raise ContentExtensionMismatch(
                    "This content can't be saved with this extension."
                )
        elif content_type == ContentType.DICT:
            if format == "json":
                with open(path, "r") as f:
                    data = json.load(f)
            else:
                raise ContentExtensionMismatch(
                    "This content can't be saved with this extension."
                )
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
        self._mkdirs(path)
        if content_type == ContentType.DF:
            if format == "csv":
                content.to_csv(path, index=False)
            elif format == "parquet":
                content.to_parquet(path, index=False)
            else:
                raise ContentExtensionMismatch(
                    "This content can't be saved with this extension."
                )
        elif content_type == ContentType.DICT:
            if format == "json":
                with open(path, "w") as f:
                    json.dump(content, f)
            else:
                raise ContentExtensionMismatch(
                    "This content can't be saved with this extension."
                )

    def _mkdirs(self, path: str):
        os.makedirs(os.path.dirname(path), exist_ok=True)

    def _compose_path(
        self, project_name: str, layer_name: str, id: str, format: str
    ) -> str:
        file_path = (
            f"{os.path.join(self._dir_path, project_name, layer_name, id)}.{format}"
        )
        return file_path
