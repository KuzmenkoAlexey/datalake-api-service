import uuid

from fastapi import HTTPException, status

from api.models import BlobCreate, FullProjectStructure, Tag
from database.db import get_first_step_collection
from shared.data_processors.base_data_processor import ProcessedData


class BaseBlobHandler:
    async def insert_blob(
        self, full_project_structure: FullProjectStructure, blob_create: BlobCreate
    ) -> str:
        first_step_collection = get_first_step_collection()
        blob_id = str(uuid.uuid4())
        blob_d = blob_create.dict()

        await first_step_collection.insert_one(
            {
                "id": blob_id,
                "name": blob_d["name"],
                "type": blob_d["content_type"],
                "size": 0,
                "timestamp": blob_d["timestamp"],
                "source": blob_d["source"],
                "user_tags": blob_d["user_tags"],
            }
        )
        return blob_id

    async def get_blob_from_first_step(self, blob_id: uuid.UUID | str):
        first_step_collection = get_first_step_collection()
        blob_d = await first_step_collection.find_one({"id": str(blob_id)})
        if not blob_d:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
        del blob_d["_id"]
        return blob_d

    async def delete_blob_from_first_step(self, blob_id: uuid.UUID | str):
        first_step_collection = get_first_step_collection()
        await first_step_collection.delete_one({"id": str(blob_id)})

    async def update_blob_data(
        self,
        full_project_structure: FullProjectStructure,
        processed_data: ProcessedData,
        blob_id: str,
    ):
        raise NotImplementedError()

    async def search_by_tags(
        self, full_project_structure: FullProjectStructure, tags: list[Tag]
    ):
        raise NotImplementedError()
