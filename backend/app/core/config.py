from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Auto Analytics AI API"
    app_env: str = "development"
    api_v1_prefix: str = "/api/v1"
    secret_key: str = "change-me-in-production"
    access_token_expire_minutes: int = 1440
    database_url: str = "sqlite:///./auto_analytics_ai.db"
    cors_origins: str = "http://localhost:3000,http://127.0.0.1:3000"
    cors_origin_regex: str = (
        r"^https://.*\.vercel\.app$|"
        r"^http://localhost(:\d+)?$|"
        r"^http://127\.0\.0\.1(:\d+)?$|"
        r"^http://192\.168\.\d+\.\d+(:\d+)?$|"
        r"^http://10\.\d+\.\d+\.\d+(:\d+)?$|"
        r"^http://172\.(1[6-9]|2\d|3[0-1])\.\d+\.\d+(:\d+)?$"
    )
    report_base_url: str = "http://localhost:3000"
    storage_root: str = "./storage"
    max_upload_size_mb: int = 200
    upload_chunk_size_bytes: int = 8 * 1024 * 1024
    preview_sample_rows: int = 5000
    analytics_sample_rows: int = 10000
    chart_sample_rows: int = 5000
    small_file_threshold_mb: int = 10
    medium_file_threshold_mb: int = 100
    small_excel_threshold_mb: int = 10
    medium_excel_threshold_mb: int = 50
    max_table_page_size: int = 100
    default_table_page_size: int = 25
    background_worker_count: int = 4
    excel_chunk_rows: int = 10000
    max_chart_count: int = 8
    background_job_backend: str = "threadpool"
    celery_broker_url: str | None = None
    celery_result_backend: str | None = None
    celery_queue_name: str = "analytics"
    job_stale_after_minutes: int = 60
    storage_backend: str = "local"
    s3_bucket_name: str | None = None
    s3_region: str | None = None
    s3_endpoint_url: str | None = None
    s3_access_key_id: str | None = None
    s3_secret_access_key: str | None = None
    s3_prefix: str = "uploads"
    s3_presign_expiry_seconds: int = 900
    s3_multipart_chunk_size_mb: int = 8
    s3_force_path_style: bool = False
    s3_use_ssl: bool = True
    upload_session_expiry_minutes: int = 1440
    upload_cleanup_interval_minutes: int = 30

    analytics_memory_limit_mb: int = 4096

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @property
    def cors_origin_list(self) -> list[str]:
        return [origin.strip() for origin in self.cors_origins.split(",") if origin.strip()]

    @property
    def resolved_storage_root(self) -> Path:
        return Path(self.storage_root).resolve()

    @property
    def max_upload_size_bytes(self) -> int:
        return self.max_upload_size_mb * 1024 * 1024

    @property
    def job_stale_after_seconds(self) -> int:
        return self.job_stale_after_minutes * 60

    @property
    def uses_celery_workers(self) -> bool:
        return self.background_job_backend.strip().lower() == "celery" and bool(self.celery_broker_url)

    @property
    def uses_s3_storage(self) -> bool:
        return self.storage_backend.strip().lower() == "s3" and bool(self.s3_bucket_name)

    @property
    def s3_multipart_chunk_size_bytes(self) -> int:
        return max(self.s3_multipart_chunk_size_mb, 5) * 1024 * 1024

    @property
    def upload_session_expiry_seconds(self) -> int:
        return self.upload_session_expiry_minutes * 60


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
