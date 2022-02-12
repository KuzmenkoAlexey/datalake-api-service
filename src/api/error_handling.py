from fastapi import Request, status
from fastapi.responses import JSONResponse

from api.dependencies import UnauthenticatedException


async def handle_unauthenticated_exception(
    request: Request, exc: UnauthenticatedException
) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_403_FORBIDDEN,
        content={"detail": exc.message or "Forbidden"},
    )
