import datetime
import typing
from enum import Enum

from pydantic import UUID4, BaseModel, Field, constr, stricturl


class Tag(BaseModel):
    name: str
    value: typing.Optional[str]


class BlobCreate(BaseModel):
    name: str
    content_type: typing.Optional[str] = "application/json"
    timestamp: typing.Optional[str] = Field(
        default_factory=lambda: datetime.datetime.now().isoformat()
    )
    source: typing.Optional[str] = ""
    user_tags: typing.Optional[list[Tag]] = []
    system_tags: typing.Optional[list[Tag]] = []
    size: typing.Optional[int] = 0  # in bytes


class Blob(BlobCreate):
    name: typing.Optional[str] = ""
    blob_id: str  # UUID?


class BlobDataJson(BaseModel):
    data: typing.Optional[dict] = None


class BlobDataBinary(BaseModel):
    data: typing.Optional[dict] = None


class ServiceProviderType(str, Enum):
    AWS = "AWS"
    AZURE = "AZURE"
    GCP = "GCP"


class Project(BaseModel):
    verified: typing.Optional[bool] = False
    name: str
    service_provider: constr(
        regex=rf"^({'|'.join([e.value for e in ServiceProviderType])})$"  # noqa
    )


class GCPCredentials(BaseModel):
    type: constr(max_length=256)
    project_id: constr(max_length=256)
    private_key_id: constr(min_length=40, max_length=40)
    private_key: constr(max_length=2096)
    client_email: constr(max_length=256)
    client_id: constr(min_length=21, max_length=21, regex=r"^\d+$")  # noqa
    auth_uri: stricturl(max_length=256)
    token_uri: stricturl(max_length=256)
    auth_provider_x509_cert_url: stricturl(max_length=256)
    client_x509_cert_url: stricturl(max_length=256)


class AWSCredentials(BaseModel):
    access_key_id: constr(max_length=256)
    secret_access_key: constr(max_length=256)


class AzureCredentials(BaseModel):
    tenant_id: constr(max_length=256)
    client_id: constr(max_length=100)
    client_secret: constr(max_length=100)


class AWSProjectDeployType(str, Enum):
    AWS_1 = "AWS_1"


class GCPProjectDeployType(str, Enum):
    GCP_1 = "GCP_1"
    GCP_2 = "GCP_2"


class AzureProjectDeployType(str, Enum):
    AZURE_1 = "AZURE_1"


class ProjectDeploy(BaseModel):
    deploy_type: constr(
        regex=rf"^({'|'.join([e.value for e in (*AWSProjectDeployType, *GCPProjectDeployType, *AzureProjectDeployType)])})$"  # noqa
    )
    project_structure: typing.Optional[dict]


class FullProjectStructure(BaseModel):
    project: Project
    credentials: typing.Union[GCPCredentials, AWSCredentials, AzureCredentials]
    deploy: ProjectDeploy


#########################
# JwtUserData ###########
#########################
class JwtUserData(BaseModel):
    user_id: UUID4
