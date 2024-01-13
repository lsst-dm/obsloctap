"""The main application factory for the obsloctap service.

Notes
-----
Be aware that, following the normal pattern for FastAPI services, the app is
constructed when this module is loaded and is not deferred until a function is
called.
"""
import logging
import os
from importlib.metadata import metadata, version

from fastapi import FastAPI
from safir.dependencies.http_client import http_client_dependency
from safir.logging import configure_logging, configure_uvicorn_logging
from safir.middleware.x_forwarded import XForwardedMiddleware

from .config import config
from .handlers.external import external_router
from .handlers.internal import internal_router

__all__ = ["app", "config"]


configure_logging(
    profile=config.profile,
    log_level=config.log_level,
    name="obsloctap",
)
configure_uvicorn_logging(config.log_level)

log = logging.getLogger(__name__)

app = FastAPI(
    title="obsloctap",
    description=metadata("obsloctap")["Summary"],
    version=version("obsloctap"),
    openapi_url=f"/{config.path_prefix}/openapi.json",
    docs_url=f"/{config.path_prefix}/docs",
    redoc_url=f"/{config.path_prefix}/redoc",
)
"""The main FastAPI application for obsloctap."""

# Attach the routers.
app.include_router(internal_router)
app.include_router(external_router, prefix=config.path_prefix)

print("Chekcing .lsst")
dir = "/home/worker/.lsst/"
if os.path.isdir(dir):
    logging.info(os.listdir("/home/worker/.lsst/"))

# Add middleware.
app.add_middleware(XForwardedMiddleware)


@app.on_event("startup")
async def startup_event() -> None:
    pass


@app.on_event("shutdown")
async def shutdown_event() -> None:
    await http_client_dependency.aclose()
