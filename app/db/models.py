from datetime import datetime
import uuid

from sqlalchemy import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from uuid_extensions import uuid7


class Base(DeclarativeBase):
    pass


class ImageThumbnailResult(Base):
    __tablename__ = "image_thumbnail_result"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid7
    )
    thumbnail_created: Mapped[bool] = mapped_column(default=False)
    gid: Mapped[uuid.UUID] = mapped_column(default=None)
    message_received_time: Mapped[datetime] = mapped_column(nullable=False)
    file_received_time: Mapped[datetime] = mapped_column(nullable=False)
    created_time: Mapped[datetime] = mapped_column(nullable=False)

    def __repr__(self) -> str:
        return (
            f"ImageThumbnailResult(id={self.id!r}, thumbnail_created={self.thumbnail_created!r}, "
            f"gid={self.gid!r})"
        )
