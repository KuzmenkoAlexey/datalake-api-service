from pydantic import BaseSettings


class Settings(BaseSettings):
    database_url: str
    database_name: str = "database_name"
    debug: bool = False
    jwt_secret: str = "SECRET"
    sentry_url: str = None
    deploy_service_base_url: str = "http://datalake-deploy-service:8000"
    gcp_credentials_path: str | None = None
    time_tracking_bigquery_dataset: str | None = None
    time_tracking_bigquery_table: str | None = None


settings = Settings()
