import uuid

from google.cloud import bigquery
from google.oauth2 import service_account
from pydantic import BaseModel

from api.models import BlobCreate, FullProjectStructure
from shared.blob_data_handlers.base import BaseBlobHandler
from shared.data_processors.base_data_processor import ProcessedData
from utils.gcp import get_credentials_tmp_path


class BigQueryResource(BaseModel):
    project: str
    dataset: str
    table: str


class GCPDeployedResources1(BaseModel):
    bigquery: BigQueryResource


class GCPBlobHandler1(BaseBlobHandler):
    async def insert_blob(
        self, full_project_structure: FullProjectStructure, blob_create: BlobCreate
    ) -> str:
        deployed_resources = GCPDeployedResources1(
            **full_project_structure.deploy.project_structure
        )
        gcp_credentials_path = get_credentials_tmp_path(
            full_project_structure.credentials
        )
        credentials = service_account.Credentials.from_service_account_file(
            filename=gcp_credentials_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        client = bigquery.Client(credentials=credentials)
        blob_id = str(uuid.uuid4())
        blob_d = blob_create.dict()
        table_id = (
            f"{deployed_resources.bigquery.project}."
            f"{deployed_resources.bigquery.dataset}."
            f"{deployed_resources.bigquery.table}"
        )
        client.insert_rows_json(
            table_id,
            [
                {
                    "id": blob_id,
                    "file": "",
                    "name": blob_d["name"],
                    "type": blob_d["content_type"],
                    "size": 0,
                    "timestamp": blob_d["timestamp"],
                    "source": blob_d["source"],
                    "tags": blob_d["tags"],
                }
            ],
        )
        return blob_id

    async def update_blob_data(
        self,
        full_project_structure: FullProjectStructure,
        processed_data: ProcessedData,
        blob_id: str,
    ):

        deployed_resources = GCPDeployedResources1(
            **full_project_structure.deploy.project_structure
        )
        gcp_credentials_path = get_credentials_tmp_path(
            full_project_structure.credentials
        )
        credentials = service_account.Credentials.from_service_account_file(
            filename=gcp_credentials_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        client = bigquery.Client(credentials=credentials)
        table_id = (
            f"{deployed_resources.bigquery.project}."
            f"{deployed_resources.bigquery.dataset}."
            f"{deployed_resources.bigquery.table}"
        )

        sql = f"UPDATE {table_id} SET file = @file WHERE id = @f_id"
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("file", "BYTES", processed_data.data),
                bigquery.ScalarQueryParameter("f_id", "STRING", blob_id),
            ]
        )
        client.query(sql, job_config=job_config)
