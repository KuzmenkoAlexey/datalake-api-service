import sentry_sdk
from fastapi import FastAPI
from sentry_sdk.integrations.asgi import SentryAsgiMiddleware

from api.dependencies import UnauthenticatedException
from api.error_handling import handle_unauthenticated_exception
from api.router.blob import blob_router
from config import settings
from shared.gcp_time_tracking import TimeTrackingBigQuery
from utils.logger import setup_logger

LOGGER = setup_logger()


def get_application():
    sentry_sdk.init(dsn=settings.sentry_url, traces_sample_rate=1.0)

    app = FastAPI(title="API Service")
    app.add_exception_handler(
        UnauthenticatedException, handle_unauthenticated_exception
    )

    app.include_router(blob_router)

    TimeTrackingBigQuery.initialize()

    return SentryAsgiMiddleware(app)
