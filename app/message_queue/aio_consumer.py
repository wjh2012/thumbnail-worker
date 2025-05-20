import io
import json
import uuid
from datetime import datetime

import aio_pika
import logging

from PIL import Image
from aio_pika.abc import AbstractIncomingMessage

from app.config.env_config import get_settings
from app.service.thumbnail_service import ThumbnailService
from app.storage.aio_boto import AioBoto
from app.db.database import AsyncSessionLocal
from app.db.models import ImageThumbnailResult

config = get_settings()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class AioConsumer:
    def __init__(
        self,
        minio_manager: AioBoto,
        thumbnail_service: ThumbnailService,
    ):
        self.minio_manager = minio_manager
        self.thumbnail_service = thumbnail_service

        self.amqp_url = f"amqp://{config.rabbitmq_user}:{config.rabbitmq_password}@{config.rabbitmq_host}:{config.rabbitmq_port}/"

        self.consume_exchange_name = config.rabbitmq_image_thumbnail_consume_exchange
        self.consume_queue_name = config.rabbitmq_image_thumbnail_consume_queue
        self.consume_routing_key = config.rabbitmq_image_thumbnail_consume_routing_key

        self.publish_exchange_name = config.rabbitmq_image_thumbnail_publish_exchange
        self.publish_routing_key = config.rabbitmq_image_thumbnail_publish_routing_key

        self.prefetch_count = 1

        self.dlx_name = config.rabbitmq_image_thumbnail_dlx
        self.dlx_routing_key = config.rabbitmq_image_thumbnail_dlx_routing_key

        self._connection = None
        self._channel = None

        self._consume_exchange = None
        self._consume_queue = None

        self._publish_exchange = None

        self._dlx = None
        self._dlq = None

    async def connect(self):
        self._connection = await aio_pika.connect_robust(self.amqp_url)
        self._channel = await self._connection.channel()

        await self._channel.set_qos(prefetch_count=self.prefetch_count)

        self._publish_exchange = await self._channel.declare_exchange(
            self.publish_exchange_name, aio_pika.ExchangeType.DIRECT, durable=True
        )
        self._consume_exchange = await self._channel.get_exchange(
            self.consume_exchange_name
        )

        self._dlx = await self._channel.declare_exchange(
            self.dlx_name, aio_pika.ExchangeType.DIRECT
        )

        args = {
            "x-dead-letter-exchange": self.dlx_name,
            "x-dead-letter-routing-key": self.dlx_routing_key,
            "x-message-ttl": 10000,  # 10초
        }

        self._consume_queue = await self._channel.declare_queue(
            self.consume_queue_name, durable=True, arguments=args
        )
        await self._consume_queue.bind(
            self._consume_exchange, routing_key=self.consume_routing_key
        )
        logging.info(
            f"✅ RabbitMQ 연결 성공: {self.amqp_url}, 큐: {self.consume_queue_name}"
        )

    async def on_message(self, message: AbstractIncomingMessage) -> None:
        async with message.process(requeue=True):
            message_received_time = datetime.now()
            logging.info("📩 메시지 수신!")

            # 메시지 파싱 및 검증
            try:
                data = json.loads(message.body)
                gid = uuid.UUID(data["gid"])
                original_object_key = data["original_object_key"]
                bucket_name = data["bucket"]
            except (ValueError, KeyError, json.JSONDecodeError) as e:
                logging.error(f"❌ 메시지 파싱 실패: {e}")
                return

            file_obj = io.BytesIO()
            try:
                await self.minio_manager.download_image_with_client(
                    bucket_name=bucket_name, key=original_object_key, file_obj=file_obj
                )
                file_received_time = datetime.now()
                file_length = file_obj.getbuffer().nbytes
                logging.info(f"✅ MinIO 파일 다운로드 성공: Size: {file_length} bytes")

                file_obj.seek(0)
                image = Image.open(file_obj)
                image.verify()
                file_obj.seek(0)
                image = Image.open(file_obj)

            except Exception as e:
                logging.error(f"❌ 이미지 로딩 실패: {e}")
                return

            try:
                # 썸네일 생성
                thumbnail_image = self.thumbnail_service.generate_small_thumbnail(image)

                # 썸네일 메모리 저장
                thumbnail_buffer = io.BytesIO()
                image_format = image.format or "JPEG"  # 포맷이 없을 경우 기본값
                thumbnail_image.save(thumbnail_buffer, format=image_format)
                thumbnail_buffer.seek(0)

                # 썸네일 키 생성
                original_filename = original_object_key.split("/")[-1]
                _, ext = original_filename.rsplit(".", 1)
                thumbnail_object_key = self.thumbnail_service.generate_thumbnail_object_key(
                    gid=gid, ext=ext
                )
                await self.minio_manager.upload_image_with_client(
                    bucket_name=bucket_name, key=thumbnail_object_key, file=thumbnail_buffer
                )
            except Exception as e:
                logging.error(f"❌ 썸네일 생성 실패: {e}")
                return
            finally:
                file_obj.close()

            created_time = datetime.now()

            try:
                async with AsyncSessionLocal() as session:
                    thumbnail_result_orm = ImageThumbnailResult(
                        gid=gid,
                        thumbnail_created=True,
                        thumbnail_object_key=thumbnail_object_key,
                        message_received_time=message_received_time,
                        file_received_time=file_received_time,
                        created_time=created_time,
                    )
                    session.add(thumbnail_result_orm)
                    await session.commit()
                    logging.info("✅ DB에 정보 저장 완료")

                await self.publish_message(
                    message_body={
                        "gid": str(gid),
                        "status": "success",
                        "thumbnail_object_key": thumbnail_object_key,
                        "created_time": created_time.isoformat(),
                    },
                )
            except Exception as e:
                logging.error(f"DB 저장 실패: {e}")

                try:
                    await self.minio_manager.delete_object(
                        bucket_name=bucket_name,
                        key=thumbnail_object_key
                    )
                    logging.info(f"🗑️ 썸네일 삭제 완료: {thumbnail_object_key}")

                except Exception as delete_err:
                    logging.error(f"❌ 썸네일 삭제 실패: {delete_err}")

                await self.publish_message(
                    message_body={
                        "gid": str(gid),
                        "status": "error",
                        "created_time": created_time.isoformat(),
                    },
                )


    async def publish_message(self, message_body: dict):
        await self._publish_exchange.publish(
            aio_pika.Message(
                body=json.dumps(message_body).encode(),
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=self.publish_routing_key,
        )
        logging.info(f"📤 메시지 발행 완료: {self.publish_routing_key}")

    async def consume(self):
        if not self._consume_queue:
            await self.connect()

        logging.info(f"📡 큐({self.consume_queue_name})에서 메시지 소비 시작...")
        await self._consume_queue.consume(self.on_message, no_ack=False)

    async def close(self):
        if self._connection:
            await self._connection.close()
            logging.info("🔴 RabbitMQ 연결 종료")
