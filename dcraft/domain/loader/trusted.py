from datetime import datetime
from typing import Any, List, Optional
from uuid import uuid4

from dcraft.domain.layer.trusted import TrustedLayerData


def create_trusted(
    content: Any,
    project_name: str,
    author: Optional[str] = None,
    description: Optional[str] = None,
    extra_info: Optional[dict] = None,
    source_ids: Optional[List[str]] = None,
) -> TrustedLayerData:
    id = str(uuid4())
    return TrustedLayerData(
        None,
        project_name,
        content,
        author,
        datetime.now(),
        description,
        extra_info,
        source_ids,
    )
