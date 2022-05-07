from azure.cosmos import CosmosClient
from azure.storage.blob import BlobServiceClient

from api.models import FullProjectStructure, Tag
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
        connection_string = ""
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
        bsc = BlobServiceClient.from_connection_string(connection_string)
        cc = bsc.get_container_client("datalake")
        cc.upload_blob(blob_id, processed_data.data)
        await self.delete_blob_from_first_step(blob_id)

    async def search_by_tags(
        self, full_project_structure: FullProjectStructure, tags: list[Tag]
    ):
        pass
