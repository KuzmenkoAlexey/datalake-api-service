from boto3 import client
from botocore.client import Config
from opensearchpy import OpenSearch, RequestsHttpConnection
from pydantic import BaseModel
from requests_aws4auth import AWS4Auth

from api.models import Blob, FullProjectStructure, Tag
from shared.blob_data_handlers.base import BaseBlobHandler
from shared.data_processors.base_data_processor import ProcessedData
from utils.logger import setup_logger

LOGGER = setup_logger()


class S3DeployedResource(BaseModel):
    bucket_name: str


class OpenSearchResource(BaseModel):
    domain_name: str
    endpoint: str


class AWSDeployedResources1(BaseModel):
    s3: S3DeployedResource
    opensearch: OpenSearchResource


class AWSBlobHandler1(BaseBlobHandler):
    async def update_blob_data(
        self,
        full_project_structure: FullProjectStructure,
        processed_data: ProcessedData,
        blob_id: str,
    ):
        blob_d = await self.get_blob_from_first_step(blob_id)
        deployed_resources = AWSDeployedResources1(
            **full_project_structure.deploy.project_structure
        )
        kwargs = {
            "aws_access_key_id": full_project_structure.credentials.access_key_id,
            "aws_secret_access_key": full_project_structure.credentials.secret_access_key,
            # TODO:
            "config": Config(region_name="us-east-1"),
        }
        s3_client = client("s3", **kwargs)

        s3_client.put_object(
            Body=processed_data.data,
            Bucket=deployed_resources.s3.bucket_name,
            Key=blob_id,
        )

        awsauth = AWS4Auth(
            full_project_structure.credentials.access_key_id,
            full_project_structure.credentials.secret_access_key,
            "us-east-1",
            "es",
            session_token=None,
        )
        search = OpenSearch(
            hosts=[{"host": deployed_resources.opensearch.endpoint, "port": 443}],
            http_auth=awsauth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
        )
        system_tags = processed_data.dict()["system_tags"]
        blob_d["system_tags"] = system_tags

        for st in system_tags:
            if st["name"] == "content-length":
                blob_d["size"] = st["value"]
        search.index(index="test", doc_type="_doc", body=blob_d, id=blob_id)
        await self.delete_blob_from_first_step(blob_id)

    async def search_by_tags(
        self, full_project_structure: FullProjectStructure, tags: list[Tag]
    ) -> list[Blob]:
        deployed_resources = AWSDeployedResources1(
            **full_project_structure.deploy.project_structure
        )
        awsauth = AWS4Auth(
            full_project_structure.credentials.access_key_id,
            full_project_structure.credentials.secret_access_key,
            "us-east-1",
            "es",
            session_token=None,
        )
        search = OpenSearch(
            hosts=[{"host": deployed_resources.opensearch.endpoint, "port": 443}],
            http_auth=awsauth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
        )
        must_query = []
        for t in tags:
            must_query.append(
                {
                    "bool": {
                        "should": [
                            {
                                "bool": {
                                    "must": [
                                        {"match": {"user_tags.name": t.name}},
                                        {"match": {"user_tags.value": t.value}},
                                    ]
                                }
                            },
                            {
                                "bool": {
                                    "must": [
                                        {"match": {"system_tags.name": t.name}},
                                        {"match": {"system_tags.value": t.value}},
                                    ]
                                }
                            },
                        ]
                    }
                }
            )
        response = search.search(
            index="test",
            doc_type="_doc",
            body={"query": {"bool": {"must": must_query}}},
            size=1500,
        )
        return [
            Blob(**hit["_source"], blob_id=hit["_source"]["id"])
            for hit in response["hits"]["hits"]
        ]
