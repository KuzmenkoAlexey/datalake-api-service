from api.models import (
    AWSProjectDeployType,
    AzureProjectDeployType,
    GCPProjectDeployType,
)
from shared.blob_data_handlers.aws_data_handlers.aws_1 import AWSBlobHandler1
from shared.blob_data_handlers.aws_data_handlers.aws_2 import AWSBlobHandler2
from shared.blob_data_handlers.azure_data_handlers.azure_1 import AzureBlobHandler1
from shared.blob_data_handlers.gcp_data_handlers.gcp_1 import GCPBlobHandler1
from shared.blob_data_handlers.gcp_data_handlers.gcp_2 import GCPBlobHandler2
from shared.blob_data_handlers.gcp_data_handlers.gcp_3 import GCPBlobHandler3

BLOB_HANDLER_CLASSES = {
    AWSProjectDeployType.AWS_1: AWSBlobHandler1,
    AWSProjectDeployType.AWS_2: AWSBlobHandler2,
    GCPProjectDeployType.GCP_1: GCPBlobHandler1,
    GCPProjectDeployType.GCP_2: GCPBlobHandler2,
    GCPProjectDeployType.GCP_3: GCPBlobHandler3,
    AzureProjectDeployType.AZURE_1: AzureBlobHandler1,
}
