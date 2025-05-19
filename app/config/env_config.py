from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    run_mode: str

    minio_host: str
    minio_port: int
    minio_user: str
    minio_password: str

    rabbitmq_host: str
    rabbitmq_port: int
    rabbitmq_user: str
    rabbitmq_password: str

    rabbitmq_image_thumbnail_consume_exchange: str
    rabbitmq_image_thumbnail_consume_queue: str
    rabbitmq_image_thumbnail_consume_routing_key: str

    rabbitmq_image_thumbnail_publish_exchange: str
    rabbitmq_image_thumbnail_publish_routing_key: str

    rabbitmq_image_thumbnail_dlx: str
    rabbitmq_image_thumbnail_dlx_routing_key: str

    model_config = SettingsConfigDict(env_file=".env")

    database_url: str
    alembic_database_url: str


@lru_cache
def get_settings():
    return Settings()
