import uuid

from boto3 import client
from botocore.client import Config
from opensearchpy import OpenSearch, RequestsHttpConnection
from pydantic import BaseModel
from requests_aws4auth import AWS4Auth

from api.models import BlobCreate, FullProjectStructure, Blob, Tag
from shared.blob_data_handlers.base import BaseBlobHandler
from shared.data_processors.base_data_processor import ProcessedData


class S3DeployedResource(BaseModel):
    bucket_name: str


class OpenSearchResource(BaseModel):
    domain_name: str


class AWSDeployedResources1(BaseModel):
    s3: S3DeployedResource
    opensearch: OpenSearchResource


class AWSBlobHandler1(BaseBlobHandler):
    async def insert_blob(
        self, full_project_structure: FullProjectStructure, blob_create: BlobCreate
    ) -> str:
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
            hosts=[
                {
                    # TODO: get this host from ARN
                    "host": (
                        f"search-{deployed_resources.opensearch.domain_name}-"
                        f"twwhb2t43nyoszfck3xw5wc3xi.us-east-1.es.amazonaws.com"
                    ),
                    "port": 443,
                }
            ],
            http_auth=awsauth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
        )
        doc = blob_create.dict()
        blob_id = str(uuid.uuid4())
        doc["id"] = blob_id
        search.index(index="test", doc_type="_doc", body=doc, id=blob_id)
        return blob_id

    async def update_blob_data(
        self,
        full_project_structure: FullProjectStructure,
        processed_data: ProcessedData,
        blob_id: str,
    ):
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
            hosts=[
                {
                    # TODO: get this host from ARN
                    "host": (
                        f"search-{deployed_resources.opensearch.domain_name}-"
                        f"twwhb2t43nyoszfck3xw5wc3xi.us-east-1.es.amazonaws.com"
                    ),
                    "port": 443,
                }
            ],
            http_auth=awsauth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
        )
        system_tags = processed_data.dict()["system_tags"]
        new_data = {"system_tags": system_tags}

        for st in system_tags:
            if st["name"] == "content-length":
                new_data["size"] = st["value"]

        search.update(
            index="test",
            doc_type="_doc",
            id=blob_id,
            body={"doc": new_data},
        )

    async def search_by_tags(
        self, full_project_structure: FullProjectStructure, tags: list[Tag]
    ) -> list[Blob]:
        # TODO:
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
            hosts=[
                {
                    # TODO: get this host from ARN
                    "host": (
                        f"search-{deployed_resources.opensearch.domain_name}-"
                        f"twwhb2t43nyoszfck3xw5wc3xi.us-east-1.es.amazonaws.com"
                    ),
                    "port": 443,
                }
            ],
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
        )
        return [
            Blob(**hit["_source"], blob_id=hit["_source"]["id"])
            for hit in response["hits"]["hits"]
        ]
