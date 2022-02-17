import json
import uuid

from google.cloud import bigquery
from google.cloud.bigquery._helpers import _bytes_to_json
from google.oauth2 import service_account
from pydantic import BaseModel

from api.models import Blob, BlobCreate, FullProjectStructure, Tag
from database.db import get_gcp1_collection
from shared.blob_data_handlers.base import BaseBlobHandler
from shared.data_processors.base_data_processor import ProcessedData
from utils.gcp import get_credentials_tmp_path
from utils.logger import setup_logger

LOGGER = setup_logger()


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
        gcp1_collection = get_gcp1_collection()
        blob_id = str(uuid.uuid4())
        blob_d = blob_create.dict()

        await gcp1_collection.insert_one(
            {
                "id": blob_id,
                "file": "",
                "name": blob_d["name"],
                "type": blob_d["content_type"],
                "size": 0,
                "timestamp": blob_d["timestamp"],
                "source": blob_d["source"],
                "user_tags": blob_d["user_tags"],
            }
        )
        return blob_id

    async def update_blob_data(
        self,
        full_project_structure: FullProjectStructure,
        processed_data: ProcessedData,
        blob_id: str,
    ):
        gcp1_collection = get_gcp1_collection()
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

        blob_d = await gcp1_collection.find_one({"id": blob_id})
        del blob_d["_id"]
        blob_d["file"] = _bytes_to_json(processed_data.data)
        blob_d["size"] = len(processed_data.data)
        blob_d["system_tags"] = [tag.dict() for tag in processed_data.system_tags]
        errors = client.insert_rows_json(table_id, [blob_d])
        if len(errors) == 0:
            await gcp1_collection.delete_one({"id": blob_id})

        for error in errors:
            LOGGER.error(f"Insert error: {error}")

    async def search_by_tags(
        self, full_project_structure: FullProjectStructure, tags: list[Tag]
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

        sql = f"""
            SELECT DISTINCT
                id,
                file,
                table.name,
                type,
                size,
                timestamp,
                source,
                TO_JSON_STRING(table.user_tags) AS user_tags,
                TO_JSON_STRING(table.system_tags) AS system_tags
            FROM
              `{table_id}` AS table,
              UNNEST(user_tags) AS user_tags,
              UNNEST(system_tags) AS system_tags
        """
        query_parameters = []

        for i, tag in enumerate(tags):
            if i == 0:
                sql += "\nWHERE\n"
            else:
                sql += "\nAND\n"

            sql += f"""
            \n((user_tags.name = @tag_name_{i} AND user_tags.value = @tag_value_{i})
            OR (system_tags.name = @tag_name_{i} AND system_tags.value = @tag_value_{i}))
            """
            query_parameters.extend(
                (
                    bigquery.ScalarQueryParameter(f"tag_name_{i}", "STRING", tag.name),
                    bigquery.ScalarQueryParameter(
                        f"tag_value_{i}", "STRING", tag.value
                    ),
                )
            )

        job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
        res = client.query(sql, job_config=job_config)

        response = []
        for row in res:
            response.append(
                Blob(
                    blob_id=row.id,
                    name=row.name,
                    content_type=row.type,
                    timestamp=row.timestamp.isoformat(),
                    source=row.source,
                    user_tags=json.loads(row.user_tags),
                    system_tags=json.loads(row.system_tags),
                    size=row.size,
                )
            )

        return response
