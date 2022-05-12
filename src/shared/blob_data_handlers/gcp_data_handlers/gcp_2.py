from google.cloud import bigtable, storage
from google.cloud.bigtable import row_filters
from google.oauth2 import service_account
from pydantic import BaseModel

from api.models import Blob, FullProjectStructure, Tag
from shared.blob_data_handlers.base import BaseBlobHandler
from shared.data_processors.base_data_processor import ProcessedData
from utils.gcp import get_credentials_tmp_path


class BigTableResource(BaseModel):
    project: str
    instance: str
    table: str


class CloudStorage(BaseModel):
    bucket: str


class GCPDeployedResources2(BaseModel):
    bigtable: BigTableResource
    cloud_storage: CloudStorage


class GCPBlobHandler2(BaseBlobHandler):
    async def update_blob_data(
        self,
        full_project_structure: FullProjectStructure,
        processed_data: ProcessedData,
        blob_id: str,
    ):
        blob_d = await self.get_blob_from_first_step(blob_id)
        deployed_resources = GCPDeployedResources2(
            **full_project_structure.deploy.project_structure
        )
        gcp_credentials_path = get_credentials_tmp_path(
            full_project_structure.credentials
        )
        credentials = service_account.Credentials.from_service_account_file(
            filename=gcp_credentials_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        bigtable_client = bigtable.Client(credentials=credentials, admin=True)
        storage_client = storage.Client(credentials=credentials)
        bucket = storage_client.bucket(deployed_resources.cloud_storage.bucket)
        blob = bucket.blob(blob_id)
        blob.upload_from_string(processed_data.data)

        instance = bigtable_client.instance(deployed_resources.bigtable.instance)
        table = instance.table(deployed_resources.bigtable.table)

        cf_name = "ColumnFamily"

        append_row = table.append_row(blob_id)
        print(blob_d)

        append_row.append_cell_value(cf_name, "id", blob_id)
        append_row.append_cell_value(cf_name, "name", blob_d["name"])
        append_row.append_cell_value(cf_name, "type", blob_d["type"])
        append_row.append_cell_value(cf_name, "size", "0")
        append_row.append_cell_value(cf_name, "timestamp", blob_d["timestamp"])
        append_row.append_cell_value(cf_name, "source", blob_d["source"])

        for user_tag in blob_d["user_tags"]:
            append_row.append_cell_value(
                cf_name, f"user_tag{user_tag['name']}", user_tag["value"]
            )

        for tag in processed_data.system_tags:
            append_row.append_cell_value(cf_name, f"system_tag{tag.name}", tag.value)
        append_row.commit()
        await self.delete_blob_from_first_step(blob_id)

    async def search_by_tags(
        self, full_project_structure: FullProjectStructure, tags: list[Tag]
    ):
        deployed_resources = GCPDeployedResources2(
            **full_project_structure.deploy.project_structure
        )
        gcp_credentials_path = get_credentials_tmp_path(
            full_project_structure.credentials
        )
        credentials = service_account.Credentials.from_service_account_file(
            filename=gcp_credentials_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        bigtable_client = bigtable.Client(credentials=credentials, admin=True)
        instance = bigtable_client.instance(deployed_resources.bigtable.instance)
        table = instance.table(deployed_resources.bigtable.table)
        row_filters_to_chain = []
        for tag in tags:
            row_filters_to_chain.append(
                row_filters.RowFilterUnion(
                    filters=[
                        row_filters.RowFilterChain(
                            filters=[
                                row_filters.ColumnQualifierRegexFilter(
                                    f"user_tag{tag.name}"
                                ),
                                row_filters.ValueRegexFilter(tag.value),
                            ]
                        ),
                        row_filters.RowFilterChain(
                            filters=[
                                row_filters.ColumnQualifierRegexFilter(
                                    f"system_tag{tag.name}"
                                ),
                                row_filters.ValueRegexFilter(tag.value),
                            ]
                        ),
                    ]
                )
            )
        # TODO: fix multiple tags filter
        if not row_filters_to_chain:
            res = table.read_rows()
        elif len(row_filters_to_chain) == 1:
            res = table.read_rows(filter_=row_filters_to_chain[0])
        else:
            res = table.read_rows(
                filter_=row_filters.RowFilterChain(filters=row_filters_to_chain)
            )
        response = []
        for r in res:
            rr = {}
            user_tags = []
            system_tags = []
            for k, v in r.to_dict().items():
                key: str = k.decode().removeprefix("ColumnFamily:")
                v = v[0].value.decode()
                if key.startswith("user_tag"):
                    key = key.removeprefix("user_tag")
                    user_tags.append(Tag(name=key, value=v))
                elif key.startswith("system_tag"):
                    key = key.removeprefix("system_tag")
                    system_tags.append(Tag(name=key, value=v))
                else:
                    rr[key] = v
            rr["user_tags"] = user_tags
            rr["system_tags"] = system_tags
            rr["blob_id"] = r.row_key.decode()
            response.append(Blob(**rr))
        print(len(response))
        return response
