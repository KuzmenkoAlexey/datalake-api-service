from google.cloud import bigquery
from google.oauth2 import service_account

from config import settings
from utils.logger import setup_logger

LOGGER = setup_logger()


class TimeTrackingBigQuery:
    initialized = False
    client: bigquery.Client | None = None
    project: str | None = None
    dataset_name: str | None = None
    table_name: str | None = None

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
    async def track_time(
        cls,
        request_type: str,
        deploy_type: str,
        request_time: float,
        created_at: float,
        number_of_tags: int | None = None,
        number_of_blobs: int | None = None,
        content_size: int | None = None,
        content_type: str | None = None,
    ):
        if cls.client:
            errors = cls.client.insert_rows_json(
                f"{cls.project}.{cls.dataset_name}.{cls.table_name}",
                [
                    {
                        "created_at": created_at,
                        "request_type": request_type,
                        "deploy_type": deploy_type,
                        "request_time": request_time,
                        # search fields
                        "number_of_tags": number_of_tags,
                        "number_of_blobs": number_of_blobs,
                        # blob_create fields
                        "size": content_size,
                        "content_type": content_type,
                    }
                ],
            )
            if errors:
                for error in errors:
                    LOGGER.error(error)
