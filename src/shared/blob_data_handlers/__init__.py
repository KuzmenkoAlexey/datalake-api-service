from api.models import AWSProjectDeployType, GCPProjectDeployType
from shared.blob_data_handlers.aws_data_handlers.aws_1 import AWSBlobHandler1
from shared.blob_data_handlers.gcp_data_handlers.gcp_1 import GCPBlobHandler1
from shared.blob_data_handlers.gcp_data_handlers.gcp_2 import GCPBlobHandler2

BLOB_HANDLER_CLASSES = {
    AWSProjectDeployType.AWS_1.value: AWSBlobHandler1,
    GCPProjectDeployType.GCP_1.value: GCPBlobHandler1,
    GCPProjectDeployType.GCP_2.value: GCPBlobHandler2,
}
