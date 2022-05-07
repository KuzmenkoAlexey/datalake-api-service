import time
from urllib import parse

from fastapi import APIRouter, Depends, Request, status

from api.dependencies import get_current_project
from api.models import Blob, BlobCreate, FullProjectStructure, Tag
from shared.blob_data_handlers import BLOB_HANDLER_CLASSES
from shared.data_processors.general_processor import GeneralDataProcessor
from shared.gcp_time_tracking import TimeTrackingBigQuery

blob_router = APIRouter(prefix="/v1/blobs", tags=["blobs"], dependencies=[])


@blob_router.post(
    "/{project_id}", status_code=status.HTTP_201_CREATED, response_model=Blob
)
async def create_blob(
    blob: BlobCreate,
    full_project_structure: FullProjectStructure = Depends(get_current_project),
) -> Blob:
    blob_handler = BLOB_HANDLER_CLASSES[full_project_structure.deploy.deploy_type]()
    s_time = time.time()
    blob_id = await blob_handler.insert_blob(full_project_structure, blob)
    e_time = time.time()
    await TimeTrackingBigQuery.track_time(
        "create_blob", full_project_structure.deploy.deploy_type, e_time - s_time
    )
    return Blob(**blob.dict(), blob_id=blob_id)


@blob_router.post("/{project_id}/{blob_id}", status_code=status.HTTP_201_CREATED)
async def create_blob_data(
    blob_id: str,
    request: Request,
    full_project_structure: FullProjectStructure = Depends(get_current_project),
):
    # TODO:
    blob_handler = BLOB_HANDLER_CLASSES[full_project_structure.deploy.deploy_type]()
    s_time = time.time()
    processed_data = await GeneralDataProcessor().process_request(request)
    await blob_handler.update_blob_data(full_project_structure, processed_data, blob_id)
    e_time = time.time()
    system_tags = processed_data.dict()["system_tags"]
    content_size = None
    content_type = None
    for st in system_tags:
        if st["name"] == "content-length":
            content_size = st["value"]
        elif st["name"] == "content-type":
            content_type = st["value"]
    await TimeTrackingBigQuery.track_time(
        "create_blob_data",
        full_project_structure.deploy.deploy_type,
        e_time - s_time,
        content_type=content_type,
        content_size=content_size,
    )


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
    s_time = time.time()
    response = await blob_handler.search_by_tags(full_project_structure, tags)
    e_time = time.time()

    await TimeTrackingBigQuery.track_time(
        "search_by_tags",
        full_project_structure.deploy.deploy_type,
        e_time - s_time,
        len(tags),
        len(response),
    )
    return response
