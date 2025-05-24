from dataclasses import dataclass
from typing import Literal


@dataclass
class PublishMessageHeader:
    event_id: str
    event_type: Literal["image.thumbnail.result"]
    trace_id: str
    timestamp: str
    source_service: str


@dataclass
class ThumbnailServiceData:
    bucket: str
    thumbnail_object_key: str


@dataclass
class PublishMessageBody:
    gid: str
    status: Literal["success", "fail"]
    completed_at: str
    payload: ThumbnailServiceData
