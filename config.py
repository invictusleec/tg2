# 配置文件
# 这里只写骨架，后续再补充具体实现

from pydantic_settings import BaseSettings
from dotenv import load_dotenv

load_dotenv()  # 加载 .env 文件

class Settings(BaseSettings):
    # Telegram API 配置
    TELEGRAM_API_ID: int
    TELEGRAM_API_HASH: str
    STRING_SESSION: str | None = None  # 可选的StringSession，来自.env
    EXPORT_STRING_SESSION: str | None = None  # 专用于导出任务的 StringSession，避免与其他进程冲突

    # 数据库配置（只强制需要 DATABASE_URL，POSTGRES_* 仅用于可选的内置DB场景）
    DATABASE_URL: str
    POSTGRES_USER: str | None = None
    POSTGRES_PASSWORD: str | None = None
    POSTGRES_DB: str | None = None

    # 默认频道配置
    DEFAULT_CHANNELS: str

    # 日志级别
    LOG_LEVEL: str = "INFO"

    # Docker 环境标识
    DOCKER_ENV: str = "false"
    STRICT_NETDISK_ONLY: bool = False

    class Config:
        env_file = ".env"  # 指定 .env 文件
        env_file_encoding = "utf-8"

settings = Settings()