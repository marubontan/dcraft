from datetime import datetime
from typing import Optional

from dcraft.domain.layer.raw import RawLayerData
from dcraft.domain.type.content import CoveredContentType


def create_raw(
    content: CoveredContentType,
    project_name: str,
    author: Optional[str] = None,
    description: Optional[str] = None,
    extra_info: Optional[dict] = None,
) -> RawLayerData:
    """Create a RawLayerData object with the given content, project name, author, description, and extra information.

    Args:
        content (CoveredContentType): The content to be stored in the RawLayerData object.
        project_name (str): The name of the project.
        author (Optional[str], optional): The author of the content. Defaults to None.
        description (Optional[str], optional): A description of the content. Defaults to None.
        extra_info (Optional[dict], optional): Extra information related to the content. Defaults to None.

    Returns:
        RawLayerData: The created RawLayerData object.
    """
    return RawLayerData(
        None,
        project_name,
        content,
        author,
        datetime.now(),
        description,
        extra_info,
    )
