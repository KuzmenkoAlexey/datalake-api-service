from fastapi import Request

from shared.data_processors.base_data_processor import BaseDataProcessor, ProcessedData
from utils.logger import setup_logger

LOGGER = setup_logger()


class DefaultDataProcessor(BaseDataProcessor):
    async def process_request(self, request: Request) -> ProcessedData:
        LOGGER.info(request)

        raise NotImplementedError()
