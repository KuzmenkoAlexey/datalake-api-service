from urllib import parse

from fastapi import APIRouter, status, Depends, Request

from api.dependencies import get_current_project
from api.models import BlobCreate, Blob, FullProjectStructure, Tag
from shared.blob_data_handlers import BLOB_HANDLER_CLASSES
from shared.data_processors.general_processor import GeneralDataProcessor

blob_router = APIRouter(prefix="/v1/blobs", tags=["blobs"], dependencies=[])


@blob_router.post(
    "/{project_id}", status_code=status.HTTP_201_CREATED, response_model=Blob
)
async def create_blob(
    blob: BlobCreate,
    full_project_structure: FullProjectStructure = Depends(get_current_project),
) -> Blob:
    blob_handler = BLOB_HANDLER_CLASSES[full_project_structure.deploy.deploy_type]()
    blob_id = await blob_handler.insert_blob(full_project_structure, blob)
    return Blob(**blob.dict(), blob_id=blob_id)


@blob_router.post("/{project_id}/{blob_id}", status_code=status.HTTP_201_CREATED)
async def create_blob_data(
    blob_id: str,
    request: Request,
    full_project_structure: FullProjectStructure = Depends(get_current_project),
):
    # TODO:
    blob_handler = BLOB_HANDLER_CLASSES[full_project_structure.deploy.deploy_type]()
    processed_data = await GeneralDataProcessor().process_request(request)
    await blob_handler.update_blob_data(full_project_structure, processed_data, blob_id)


@blob_router.get("/{project_id}", status_code=status.HTTP_200_OK)
async def search_by_tags(
    request: Request,
    full_project_structure: FullProjectStructure = Depends(get_current_project),
) -> list[Blob]:
    # TODO:
    blob_handler = BLOB_HANDLER_CLASSES[full_project_structure.deploy.deploy_type]()
    params = dict(
        parse.parse_qsl(parse.urlsplit(str(request.url)).query, keep_blank_values=True)
    )
    tags = [Tag(name=k, value=v) for k, v in params.items()]
    response = await blob_handler.search_by_tags(full_project_structure, tags)
    return response
