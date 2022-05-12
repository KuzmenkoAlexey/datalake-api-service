from azure.cosmos import CosmosClient
from azure.storage.blob import BlobServiceClient

from api.models import Blob, FullProjectStructure, Tag
from shared.blob_data_handlers.base import BaseBlobHandler
from shared.data_processors.base_data_processor import ProcessedData
from utils.logger import setup_logger

LOGGER = setup_logger()


class AzureBlobHandler1(BaseBlobHandler):
    async def update_blob_data(
        self,
        full_project_structure: FullProjectStructure,
        processed_data: ProcessedData,
        blob_id: str,
    ):
        blob_d = await self.get_blob_from_first_step(blob_id)
        # TODO:
        blob_connection_string = ""
        client = CosmosClient(
            "",
            credential="",
        )
        db = client.create_database_if_not_exists("ToDoList")
        cont = db.get_container_client("Items")
        blob_d["id"] = blob_id
        blob_d["_rid"] = blob_id
        blob_d["_self"] = blob_id

        system_tags = processed_data.dict()["system_tags"]
        blob_d["system_tags"] = system_tags
        for st in system_tags:
            if st["name"] == "content-length":
                blob_d["size"] = st["value"]

        cont.create_item(blob_d)
        bsc = BlobServiceClient.from_connection_string(blob_connection_string)
        cc = bsc.get_container_client("datalake")
        cc.upload_blob(blob_id, processed_data.data)
        await self.delete_blob_from_first_step(blob_id)

    async def search_by_tags(
        self, full_project_structure: FullProjectStructure, tags: list[Tag]
    ):
        client = CosmosClient(
            "",
            credential="",
        )
        db = client.create_database_if_not_exists("ToDoList")
        cont = db.get_container_client("Items")

        filter_expressions = []
        for tag in tags:
            filter_expressions.append(
                f"("
                f'EXISTS(SELECT VALUE n FROM n IN c.user_tags WHERE n["name"] = "{tag.name}" AND n["value"] = "{tag.value}") OR '
                f'EXISTS(SELECT VALUE n FROM n IN c.system_tags WHERE n["name"] = "{tag.name}" AND n["value"] = "{tag.value}")'
                f")"
            )
        filter_expression = " AND ".join(filter_expressions)
        if filter_expression:
            filter_expression = "SELECT * FROM c WHERE " + filter_expression
        result = []
        for res in cont.query_items(
            filter_expression, enable_cross_partition_query=True
        ):
            result.append(
                Blob(
                    blob_id=res["id"],
                    name=res["name"],
                    content_type=res["type"],
                    timestamp=res["timestamp"],
                    source=res["source"],
                    user_tags=res["user_tags"],
                    system_tags=res["system_tags"],
                    size=res["size"],
                )
            )
        return result
