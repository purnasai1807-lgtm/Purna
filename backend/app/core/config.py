from functools import lru_cache

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

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @property
    def cors_origin_list(self) -> list[str]:
        return [origin.strip() for origin in self.cors_origins.split(",") if origin.strip()]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
