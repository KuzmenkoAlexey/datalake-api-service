import json
import uuid

from api.models import GCPCredentials


def get_credentials_tmp_path(credentials: GCPCredentials) -> str:
    gcp_credentials_path = f"/tmp/{str(uuid.uuid4()).replace('-', '')}_gcp_credentials"
    with open(gcp_credentials_path, "w") as f:
        json.dump(credentials.dict(), f)
    return gcp_credentials_path
