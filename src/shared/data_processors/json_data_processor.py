import json

from fastapi import Request

from shared.data_processors.base_data_processor import BaseDataProcessor, ProcessedData


class JsonDataProcessor(BaseDataProcessor):
    async def process_request(self, request: Request) -> ProcessedData:
        processed_data = ProcessedData(
            data=json.dumps(await request.json()).encode(), system_tags=[]
        )
        return processed_data
