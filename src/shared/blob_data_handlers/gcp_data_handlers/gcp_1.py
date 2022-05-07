import json

from google.cloud import bigquery
from google.cloud.bigquery._helpers import _bytes_to_json
from google.oauth2 import service_account
from pydantic import BaseModel

from api.models import Blob, FullProjectStructure, Tag
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
    async def update_blob_data(
        self,
        full_project_structure: FullProjectStructure,
        processed_data: ProcessedData,
        blob_id: str,
    ):

        blob_d = await self.get_blob_from_first_step(blob_id)
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

        blob_d["file"] = _bytes_to_json(processed_data.data)
        blob_d["size"] = len(processed_data.data)
        blob_d["system_tags"] = [tag.dict() for tag in processed_data.system_tags]
        errors = client.insert_rows_json(table_id, [blob_d])
        if len(errors) == 0:
            await self.delete_blob_from_first_step(blob_id)

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
