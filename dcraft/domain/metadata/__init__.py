from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from dcraft.domain.type.enum import ContentType


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
