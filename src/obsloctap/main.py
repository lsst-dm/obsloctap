# This file is part of obsloctap.
#
# Developed for the Rubin Data Management System.
# This product includes software developed by the Rubin Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# Use of this source code is governed by a 3-clause BSD-style
# license that can be found in the LICENSE file.

"""The main application factory for the obsloctap service.

Notes
-----
Be aware that, following the normal pattern for FastAPI services, the app is
constructed when this module is loaded and is not deferred until a function is
called.
"""

import logging
import os
from contextlib import asynccontextmanager
from importlib.metadata import metadata, version
from typing import AsyncIterator

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from safir.dependencies.http_client import http_client_dependency
from safir.logging import configure_logging, configure_uvicorn_logging
from safir.middleware.x_forwarded import XForwardedMiddleware
from starlette.responses import FileResponse

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


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # Startup code
    logging.info("Starting up")
    yield
    logging.info("Shutting down")
    await http_client_dependency.aclose()


app = FastAPI(
    title="obsloctap",
    description=metadata("obsloctap")["Summary"],
    version=version("obsloctap"),
    openapi_url=f"/{config.path_prefix}/openapi.json",
    docs_url=f"/{config.path_prefix}/docs",
    redoc_url=f"/{config.path_prefix}/redoc",
    lifespan=lifespan,
)
app.mount(
    f"{config.path_prefix}/static",
    StaticFiles(directory="static"),
    name="static",
)


@app.get(f"{config.path_prefix}/favicon.ico", include_in_schema=False)
async def favicon() -> FileResponse:
    return FileResponse(os.path.join("static", "favicon.ico"))


"""The main FastAPI application for obsloctap."""
# Attach the routers.
app.include_router(internal_router)
app.include_router(external_router, prefix=config.path_prefix)

app.add_middleware(XForwardedMiddleware)
