import tempfile

import exiftool
from fastapi import Request

from api.models import Tag
from shared.data_processors.base_data_processor import BaseDataProcessor, ProcessedData


def convert_metadata_to_tags_list(metadata: dict) -> list[Tag]:
    tags = []
    for key, value in metadata.items():
        tags.append(Tag(name=key, value=value))

    return tags


class ImagesJpegProcessor(BaseDataProcessor):
    async def process_request(self, request: Request) -> ProcessedData:
        image_data = await request.body()

        # exiftool necessarily needs the path to the file to work
        with tempfile.NamedTemporaryFile() as tf, exiftool.ExifTool() as et:
            tf.write(image_data)
            tf.seek(0)
            metadata = et.get_metadata(tf.name)

        tags = convert_metadata_to_tags_list(metadata)

        return ProcessedData(data=await request.body(), system_tags=tags)
