from boto3 import client, resource
from boto3.dynamodb.conditions import Attr
from botocore.client import Config
from pydantic import BaseModel

from api.models import Blob, FullProjectStructure, Tag
from shared.blob_data_handlers.base import BaseBlobHandler
from shared.data_processors.base_data_processor import ProcessedData
from utils.logger import setup_logger

LOGGER = setup_logger()


class S3DeployedResource(BaseModel):
    bucket_name: str


class DynamoDBResource(BaseModel):
    dynamodb_name: str


class AWSDeployedResources2(BaseModel):
    s3: S3DeployedResource
    dynamodb: DynamoDBResource


class AWSBlobHandler2(BaseBlobHandler):
    async def update_blob_data(
        self,
        full_project_structure: FullProjectStructure,
        processed_data: ProcessedData,
        blob_id: str,
    ):
        blob_d = await self.get_blob_from_first_step(blob_id)
        deployed_resources = AWSDeployedResources2(
            **full_project_structure.deploy.project_structure
        )
        kwargs = {
            "aws_access_key_id": full_project_structure.credentials.access_key_id,
            "aws_secret_access_key": full_project_structure.credentials.secret_access_key,
            # TODO:
            "config": Config(region_name="us-east-1"),
        }
        s3_client = client("s3", **kwargs)
        dynamodb = resource("dynamodb", **kwargs)
        dynamodb_table = dynamodb.Table(deployed_resources.dynamodb.dynamodb_name)

        s3_client.put_object(
            Body=processed_data.data,
            Bucket=deployed_resources.s3.bucket_name,
            Key=blob_id,
        )

        item = {"system_tags": processed_data.dict()["system_tags"], **blob_d}
        LOGGER.debug(f"DynamoDB item: {item}")
        dynamodb_table.put_item(Item=item)
        await self.delete_blob_from_first_step(blob_id)

    async def search_by_tags(
        self, full_project_structure: FullProjectStructure, tags: list[Tag]
    ) -> list[Blob]:
        deployed_resources = AWSDeployedResources2(
            **full_project_structure.deploy.project_structure
        )
        kwargs = {
            "aws_access_key_id": full_project_structure.credentials.access_key_id,
            "aws_secret_access_key": full_project_structure.credentials.secret_access_key,
            # TODO:
            "config": Config(region_name="us-east-1"),
        }
        dynamodb = resource("dynamodb", **kwargs)
        dynamodb_table = dynamodb.Table(deployed_resources.dynamodb.dynamodb_name)

        if tags:
            query = Attr("user_tags").contains(
                {"name": tags[0].name, "value": tags[0].value}
            ) | Attr("system_tags").contains(
                {"name": tags[0].name, "value": tags[0].value}
            )
            for i in range(1, len(tags)):
                query &= Attr("user_tags").contains(
                    {"name": tags[i].name, "value": tags[i].value}
                ) | Attr("system_tags").contains(
                    {"name": tags[i].name, "value": tags[i].value}
                )
            res = dynamodb_table.scan(FilterExpression=query)
        else:
            res = dynamodb_table.scan()

        response = []
        for item in res["Items"]:
            response.append(
                Blob(
                    blob_id=item["id"],
                    name=item["name"],
                    content_type=item["type"],
                    timestamp=item["timestamp"],
                    source=item["source"],
                    user_tags=item["user_tags"],
                    system_tags=item["system_tags"],
                    size=item["size"],
                )
            )
        return response
