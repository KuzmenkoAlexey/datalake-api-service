from pydantic import BaseSettings


class Settings(BaseSettings):
    database_url: str
    database_name: str = "database_name"
    jwt_secret: str = "SECRET"
    sentry_url: str = None
    arkham_service_base_url: str = "http://arkham:8000"


settings = Settings()
