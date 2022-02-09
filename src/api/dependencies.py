import typing
from typing import Optional

import httpx
from fastapi import Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import ExpiredSignatureError, JWTError, jwt
from pydantic import UUID4

from api.models import FullProjectStructure, JwtUserData
from config import settings


class UnauthenticatedException(Exception):
    def __init__(self, message: Optional[str] = None):
        self.message = message or None
        self.status = 401
        super().__init__(self.message)


http_bearer = HTTPBearer(auto_error=False)


async def get_current_user(
    authorization: typing.Optional[HTTPAuthorizationCredentials] = Depends(http_bearer),
) -> JwtUserData:
    if authorization is None:
        raise UnauthenticatedException()
    try:
        payload: dict = jwt.decode(
            authorization.credentials,
            settings.jwt_secret,
            algorithms=jwt.ALGORITHMS.HS256,
            audience="fastapi-users:auth",
        )
    except ExpiredSignatureError:
        print("JWT token expired")
        raise UnauthenticatedException("Token expired")
    except JWTError:
        print("Invalid JWT token")
        raise UnauthenticatedException("Invalid bearer token")
    return JwtUserData(user_id=payload.get("user_id"))


async def get_current_project(
    project_id: UUID4,
    jwt_user_data: JwtUserData = Depends(get_current_user),
    authorization: typing.Optional[HTTPAuthorizationCredentials] = Depends(http_bearer),
) -> FullProjectStructure:
    payload: dict = jwt.decode(
        authorization.credentials,
        settings.jwt_secret,
        algorithms=jwt.ALGORITHMS.HS256,
        audience="fastapi-users:auth",
    )
    payload["project_id"] = str(project_id)
    jwt_token = jwt.encode(payload, settings.jwt_secret, algorithm=jwt.ALGORITHMS.HS256)
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{settings.arkham_service_base_url}/v1/full_projects",
            headers={"Authorization": f"Bearer {jwt_token}"},
        )
    return FullProjectStructure(**resp.json())
