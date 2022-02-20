from fastapi import Request

from shared.data_processors.base_data_processor import BaseDataProcessor, ProcessedData


class TextProcessor(BaseDataProcessor):
    async def process_request(self, request: Request) -> ProcessedData:
        return ProcessedData(data=await request.body(), system_tags=[])
