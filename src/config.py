import typing

from pydantic import BaseSettings


class Settings(BaseSettings):
    database_url: str
    database_name: str = "database_name"
    jwt_secret: str = "SECRET"
    sentry_url: str = None
    arkham_service_base_url: str = "http://datalake-deploy-service:8000"
    gcp_credentials_path: typing.Optional[str] = None


settings = Settings()
