from dataclasses import dataclass
from typing import Optional


@dataclass
class ThumbnailResult:
    thumbnail_created: Optional[bool] = None
