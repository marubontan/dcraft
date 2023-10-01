from datetime import datetime
from typing import Any, List, Optional
from uuid import uuid4

from dcraft.domain.layer.refined import RefinedLayerData


def create_refined(
    content: Any,
    project_name: str,
    author: Optional[str] = None,
    description: Optional[str] = None,
    extra_info: Optional[dict] = None,
    source_ids: Optional[List[str]] = None,
) -> RefinedLayerData:
    id = str(uuid4())
    return RefinedLayerData(
        None,
        project_name,
        content,
        author,
        datetime.now(),
        description,
        extra_info,
        source_ids,
    )
