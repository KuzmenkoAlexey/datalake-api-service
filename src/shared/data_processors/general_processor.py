import enum
import typing

from fastapi import Request
from starlette.datastructures import Headers

from api.models import Tag
from shared.data_processors.base_data_processor import ProcessedData
from shared.data_processors.default_data_processor import DefaultDataProcessor
from shared.data_processors.json_data_processor import JsonDataProcessor
from shared.data_processors.multipart_data_processor import MultipartDataProcessor
from utils.logger import setup_logger

LOGGER = setup_logger()


class ContentType(str, enum.Enum):
    JSON_CONTENT_TYPE = "application/json"
    TEXT_HTML_CONTENT_TYPE = "text/html"
    TEXT_PLAIN_CONTENT_TYPE = "text/plain"
    IMAGE_JPEG_CONTENT_TYPE = "image/jpeg"
    MULTIPART_FORM_DATA_CONTENT_TYPE = "multipart/form-data"


CONTENT_TYPE_HANDLERS = {
    ContentType.JSON_CONTENT_TYPE: JsonDataProcessor,
    ContentType.MULTIPART_FORM_DATA_CONTENT_TYPE: MultipartDataProcessor,
}


class GeneralDataProcessor:
    def get_content_type_from_headers(self, headers: Headers) -> typing.Optional[str]:
        return headers.get("content-type")

    async def process_request(self, request: Request) -> ProcessedData:
        content_type = self.content_type_to_internal_content_type(
            self.get_content_type_from_headers(request.headers)
        )
        LOGGER.info(request.headers)
        LOGGER.info(content_type)
        content_type_handler = CONTENT_TYPE_HANDLERS.get(
            content_type, DefaultDataProcessor
        )
        processed_data = await content_type_handler().process_request(request)

        tags = self.extract_system_tags_from_headers(request.headers)
        processed_data.system_tags.extend(tags)
        return processed_data

    def extract_system_tags_from_headers(self, headers: Headers) -> list[Tag]:
        tags = []
        for expected_header in ["content-length", "host", "user-agent", "content-type"]:
            if expected_header in headers:
                tags.append(Tag(name=expected_header, value=headers[expected_header]))
        return tags

    def content_type_to_internal_content_type(
        self, content_type: typing.Optional[str]
    ) -> typing.Optional[ContentType]:
        if not content_type:
            return None

        if content_type == ContentType.JSON_CONTENT_TYPE:
            return ContentType.JSON_CONTENT_TYPE

        if content_type.startswith(ContentType.MULTIPART_FORM_DATA_CONTENT_TYPE):
            return ContentType.MULTIPART_FORM_DATA_CONTENT_TYPE

        if content_type == ContentType.TEXT_HTML_CONTENT_TYPE:
            return ContentType.TEXT_HTML_CONTENT_TYPE

        if content_type == ContentType.TEXT_PLAIN_CONTENT_TYPE:
            return ContentType.TEXT_PLAIN_CONTENT_TYPE

        if content_type == ContentType.IMAGE_JPEG_CONTENT_TYPE:
            return ContentType.IMAGE_JPEG_CONTENT_TYPE

        return None
