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

"""Handlers for the app's external root, ``/obsloctap/``."""

import logging

from astropy.time import Time
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
    start: str = Query(
        "now",
        description="time to start from 'now' or ISO 'YYYY-MM-DD HH:MM:SS'",
    ),
    time: int = Query(24, description="hours[1-48] for schedule lookahead"),
    logger: BoundLogger = Depends(logger_dependency),
) -> list[Obsplan]:
    logger.info(f"Schedule requested for time: {time}, start {start}")
    dbhelp = await DbHelpProvider.getHelper()
    if start and start.lower() != "now":
        t = Time(
            start, format="iso", scale="utc"
        )  # or scale='tai', etc. as appropriate
        schedule = await dbhelp.get_schedule(time, start=t)
    else:
        schedule = await dbhelp.get_schedule(time)
    return schedule
