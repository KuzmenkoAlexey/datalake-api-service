from fastapi import Request
from pydantic import BaseModel
from api.models import Tag


class ProcessedData(BaseModel):
    data: bytes
    system_tags: list[Tag]


class BaseDataProcessor:
    async def process_request(self, request: Request) -> ProcessedData:
        raise NotImplementedError()
