"""The main application factory for the obsloctap service.

Notes
-----
Be aware that, following the normal pattern for FastAPI services, the app is
constructed when this module is loaded and is not deferred until a function is
called.
"""

from importlib.metadata import metadata, version

from fastapi import FastAPI
from safir.dependencies.http_client import http_client_dependency
from safir.logging import configure_logging, configure_uvicorn_logging
from safir.middleware.x_forwarded import XForwardedMiddleware

from .config import config
from .handlers.external import external_router
from .handlers.internal import internal_router
from .models import Observation

__all__ = ["app", "config"]


configure_logging(
    profile=config.profile,
    log_level=config.log_level,
    name="obsloctap",
)
configure_uvicorn_logging(config.log_level)

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
app.include_router(external_router, prefix=f"/{config.path_prefix}")

# Add middleware.
app.add_middleware(XForwardedMiddleware)


@app.on_event("shutdown")
async def shutdown_event() -> None:
    await http_client_dependency.aclose()


@app.get("/schedule/", response_model=list[Observation])
async def get_schedule() -> any:
    observations = []
    obs = Observation(
        mjd=60032.194918981484,
        ra=90.90909091666666,
        dec=-74.60384434722222,
        rotSkyPos=18.33895879413964,
        nexp=3,
    )
    observations.append(obs)
    return observations
