import io
import logging
import os
import aioboto3
from app.config.custom_logger import time_logger
from app.config.env_config import get_settings

config = get_settings()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

class AioBoto:
    def __init__(self):
        self.minio_url = f"http://{config.minio_host}:{config.minio_port}"
        self._session = None
        self.s3_client_cm = None
        self.s3_client = None

    async def connect(self):
        if self.s3_client:
            return

        self._session = aioboto3.Session()
        self.s3_client_cm = self._session.client(
            "s3",
            endpoint_url=self.minio_url,
            aws_access_key_id=os.getenv("MINIO_USER", "admin"),
            aws_secret_access_key=os.getenv("MINIO_PASSWORD", "adminadmin"),
        )
        self.s3_client = await self.s3_client_cm.__aenter__()

    @time_logger
    async def upload_image_with_client(self, file, bucket_name: str, key: str):
        await self.connect()
        await self.s3_client.upload_fileobj(file, Bucket=bucket_name, Key=key)
        logging.info(f"✅ MinIO client 파일 업로드 성공: {key} (Bucket: {bucket_name})")

    @time_logger
    async def download_image_with_client(
        self, bucket_name: str, key: str, file_obj: io.BytesIO
    ):
        await self.connect()
        await self.s3_client.download_fileobj(
            Bucket=bucket_name, Key=key, Fileobj=file_obj
        )

    @time_logger
    async def delete_object(self, bucket_name: str, key: str):
        await self.connect()
        try:
            await self.s3_client.delete_object(Bucket=bucket_name, Key=key)
            logging.info(f"🗑️ MinIO 객체 삭제 성공: {key} (Bucket: {bucket_name})")
        except Exception as e:
            logging.error(f"❌ MinIO 객체 삭제 실패: {e}")
            raise


async def close(self):
        if self.s3_client_cm:
            await self.s3_client_cm.__aexit__(None, None, None)
        logging.info("❌ Minio 연결 종료")
