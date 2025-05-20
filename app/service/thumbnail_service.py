from datetime import datetime

from PIL.Image import Image

from app.config.env_config import get_settings

config = get_settings()

class ThumbnailService:
    def generate_small_thumbnail(self, image:Image):
        image.thumbnail((150, 150))
        return image

    def generate_thumbnail_object_key(self, gid, ext) -> str:
        gid_str = str(gid)
        short_uuid = gid_str.replace("-", "")[-8:]

        now = datetime.now()
        date_path = now.strftime("%Y/%m/%d")
        time_part = now.strftime("%H%M%S")

        prefix = config.thumbnail_image_object_key_prefix
        return f"{prefix}/{date_path}/{time_part}_{short_uuid}.{ext}"