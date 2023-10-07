from datetime import datetime
from typing import List, Optional

from dcraft.domain.layer.refined import RefinedLayerData
from dcraft.domain.type.content import CoveredContentType


def create_refined(
    content: CoveredContentType,
    project_name: str,
    author: Optional[str] = None,
    description: Optional[str] = None,
    extra_info: Optional[dict] = None,
    source_ids: Optional[List[str]] = None,
) -> RefinedLayerData:
    """Create a refined layer data object.

    Args:
        content (CoveredContentType): The content of the refined layer.
        project_name (str): The name of the project.
        author (Optional[str], optional): The author of the refined layer. Defaults to None.
        description (Optional[str], optional): The description of the refined layer. Defaults to None.
        extra_info (Optional[dict], optional): Extra information about the refined layer. Defaults to None.
        source_ids (Optional[List[str]], optional): The source IDs of the refined layer. Defaults to None.

    Returns:
        RefinedLayerData: The created refined layer data object.
    """
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
