"""Handlers for the app's external root, ``/obsloctap/``."""

import logging

from fastapi import APIRouter, Depends
from fastapi.params import Query
from safir.dependencies.logger import logger_dependency
from safir.metadata import get_metadata
from structlog.stdlib import BoundLogger

from ..config import config
from ..db import DbHelpProvider
from ..models import Index, Obsplan

__all__ = ["get_index", "external_router"]

external_router = APIRouter()
"""FastAPI router for all external handlers."""

# Disable uvicorn logging
logging.getLogger("uvicorn.access").disabled = True
logging.getLogger("uvicorn.info").disabled = True
logging.getLogger("uvicorn.event").disabled = True


@external_router.get(
    "/",
    description=("Index page, about the application."),
    response_model=Index,
    response_model_exclude_none=True,
    summary="About Rubin obsloctap",
)
async def get_index(
    logger: BoundLogger = Depends(logger_dependency),
) -> Index:
    """GET ``/`` (the app's external root).

    By convention, the root of the external API includes a field called
    ``metadata`` that provides the same Safir-generated metadata as the
    internal root endpoint.
    """
    # There is no need to log simple requests since uvicorn will do this
    # automatically, but this is included as an example of how to use the
    # logger for more complex logging.
    logger.debug("Request for application metadata")

    metadata = get_metadata(
        package_name="obsloctap",
        application_name=config.name,
    )
    page = Index(metadata=metadata)

    return page


@external_router.get(
    "/schedule",
    description="Return the curent observing schedule.",
    response_model=list[Obsplan],
    response_model_exclude_none=True,
    summary="Observation Schedule",
)
async def get_schedule(
    time: int = Query(24, description="hours[1-48] for schedule lookahead"),
    logger: BoundLogger = Depends(logger_dependency),
) -> list[Obsplan]:
    logger.info(f"Schedule requested for time: {time}")
    dbhelp = await DbHelpProvider.getHelper()
    schedule = await dbhelp.get_schedule(time)
    return schedule
