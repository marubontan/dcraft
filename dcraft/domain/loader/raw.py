from datetime import datetime
from typing import Any, Optional
from uuid import uuid4

from dcraft.domain.layer.raw import RawLayerData


def create_raw(
    content: Any,
    project_name: str,
    author: Optional[str] = None,
    description: Optional[str] = None,
    extra_info: Optional[dict] = None,
) -> RawLayerData:
    return RawLayerData(
        None,
        project_name,
        content,
        author,
        datetime.now(),
        description,
        extra_info,
    )
