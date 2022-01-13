from api.models import BlobCreate, FullProjectStructure, Tag
from shared.data_processors.base_data_processor import ProcessedData


class BaseBlobHandler:
    async def insert_blob(
        self, full_project_structure: FullProjectStructure, blob_create: BlobCreate
    ) -> str:
        raise NotImplementedError()

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
