import typing

from google.cloud import bigquery
from google.oauth2 import service_account

from config import settings
from utils.logger import setup_logger

LOGGER = setup_logger()


class TimeTrackingBigQuery:
    initialized = False
    client: typing.Optional[bigquery.Client] = None
    project: typing.Optional[str] = None
    dataset_name: typing.Optional[str] = None
    table_name: typing.Optional[str] = None

    @classmethod
    def initialize(cls):
        if (
            settings.gcp_credentials_path
            and settings.time_tracking_bigquery_dataset
            and settings.time_tracking_bigquery_table
        ):
            cls.dataset_name = settings.time_tracking_bigquery_dataset
            cls.table_name = settings.time_tracking_bigquery_table
            credentials = service_account.Credentials.from_service_account_file(
                filename=settings.gcp_credentials_path,
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
            client = bigquery.Client(credentials=credentials)
            dataset_ref = client.get_dataset(cls.dataset_name)
            table_ref = dataset_ref.table(cls.table_name)
            table = client.get_table(table_ref)
            LOGGER.debug(table)
            cls.client = client
            cls.project = cls.client.project

    @classmethod
    async def track_time(cls, request_type: str, deploy_type: str, request_time: float):
        if cls.client:
            cls.client.insert_rows_json(
                f"{cls.project}.{cls.dataset_name}.{cls.table_name}",
                [
                    {
                        "request_type": request_type,
                        "deploy_type": deploy_type,
                        "request_time": request_time,
                    }
                ],
            )
