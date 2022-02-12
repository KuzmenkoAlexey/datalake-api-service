import time

import sentry_sdk
from aioprometheus import Counter, Histogram, MetricsMiddleware, Summary
from aioprometheus.asgi.starlette import metrics
from fastapi import FastAPI, Request
from sentry_sdk.integrations.asgi import SentryAsgiMiddleware

from api.dependencies import UnauthenticatedException
from api.error_handling import handle_unauthenticated_exception
from api.router.blob import blob_router
from config import settings
from utils.logger import setup_logger

LOGGER = setup_logger()


def get_application():
    sentry_sdk.init(dsn=settings.sentry_url, traces_sample_rate=1.0)

    app = FastAPI(title="API Service")
    app.add_exception_handler(
        UnauthenticatedException, handle_unauthenticated_exception
    )
    @app.middleware("http")
    async def prometheus_stats(request: Request, call_next):
        start_time = time.time()
        response = await call_next(request)
        process_time = time.time() - start_time
        request_path = request.scope["path"]
        LOGGER.info(f"Request time: {process_time}s, path: {request_path}")
        request.app.state.http_access_time_s.observe(
            {"path": request_path}, process_time
        )
        request.app.state.http_access_time_hist.observe(
            {"path": request_path}, process_time
        )
        request.app.state.users_events_counter.inc({"path": request_path})
        return response

    app.add_middleware(MetricsMiddleware)
    app.add_route("/metrics", metrics)
    app.state.users_events_counter = Counter("events", "Number of events.")
    app.state.http_access_time_s = Summary("http_access_time_s", "HTTP access time.")
    app.state.http_access_time_hist = Histogram(
        "http_access_time_hist", "HTTP access time"
    )

    app.include_router(blob_router)

    return SentryAsgiMiddleware(app)
