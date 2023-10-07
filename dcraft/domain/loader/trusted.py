from datetime import datetime
from typing import List, Optional

from dcraft.domain.layer.trusted import TrustedLayerData
from dcraft.domain.type.content import CoveredContentType


def create_trusted(
    content: CoveredContentType,
    project_name: str,
    author: Optional[str] = None,
    description: Optional[str] = None,
    extra_info: Optional[dict] = None,
    source_ids: Optional[List[str]] = None,
) -> TrustedLayerData:
    """Creates a trusted layer data object.

    Args:
        content (CoveredContentType): The content of the trusted layer.
        project_name (str): The name of the project.
        author (Optional[str], optional): The author of the trusted layer. Defaults to None.
        description (Optional[str], optional): A description of the trusted layer. Defaults to None.
        extra_info (Optional[dict], optional): Any extra information associated with the trusted layer. Defaults to None.
        source_ids (Optional[List[str]], optional): The source IDs associated with the trusted layer. Defaults to None.

    Returns:
        TrustedLayerData: The created trusted layer data object.
    """
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
