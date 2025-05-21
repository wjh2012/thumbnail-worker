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
class PublishMessagePayload:
    gid: str
    status: str
    bucket: str
    thumbnail_object_key: str
    created_at: str
