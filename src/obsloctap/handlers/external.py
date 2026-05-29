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

import io
import json
import logging
from typing import Any, cast

from astropy.io.votable import from_table, writeto
from astropy.table import Table
from astropy.time import Time, TimeDelta
from fastapi import APIRouter, Depends, HTTPException
from fastapi.params import Query
from fastapi.responses import HTMLResponse, Response
from safir.dependencies.logger import logger_dependency
from safir.metadata import get_metadata
from structlog.stdlib import BoundLogger

from ..config import config
from ..consdbhelp import (
    ConsDbHelpProvider,
    validate_exposure_columns,
    validate_exposure_predicate,
)
from ..db import DbHelpProvider, validate_columns, validate_predicate
from ..models import Exposure, Index, Obsplan
from ..skymap import make_sky_html

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
    RESPONSEFORMAT: str = Query(
        "json",
        description="Response format: json, application/json, "
        "votable, application/x-votable+xml, text/xml, csv, text/csv",
    ),
    columns: str = Query(
        "",
        description="Comma-separated list of columns to return. "
        "Default (empty) returns all columns.",
    ),
    predicate: str = Query(
        "",
        description='Filter predicate, e.g. "s_ra > 100 AND s_dec < 50" or '
        "\"execution_status = 'Performed'\". Supports =, !=, <, >, <=, >=, "
        "LIKE, AND, OR.",
    ),
    logger: BoundLogger = Depends(logger_dependency),
) -> Response:
    logger.info(f"Schedule requested for time: {time}, start {start}")

    # Parse and validate columns parameter
    requested_cols: list[str] = []
    if columns:
        requested_cols = [c.strip() for c in columns.split(",") if c.strip()]
        try:
            validate_columns(requested_cols)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    # Validate predicate - check column names are valid
    validated_predicate: str | None = None
    if predicate:
        try:
            validated_predicate = validate_predicate(predicate)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    dbhelp = await DbHelpProvider.getHelper()
    if start and start.lower() != "now":
        t = Time.now() - TimeDelta("1440min")
        success = False
        try:
            t = Time(start, format="iso", scale="utc")
            success = True
        except Exception:
            pass
        try:
            t = Time(float(start), format="mjd")
            success = True
        except Exception:
            pass

        logger.info(f"Converted time: {success} - Using start time: {t}")
        result = await dbhelp.get_schedule(
            time,
            start=t,
            columns=requested_cols or None,
            predicate=validated_predicate,
        )
    else:
        result = await dbhelp.get_schedule(
            time,
            columns=requested_cols or None,
            predicate=validated_predicate,
        )

    # Convert to dicts - result is already dicts if columns specified
    if requested_cols:
        data: list[dict[str, Any]] = cast(list[dict[str, Any]], result)
    else:
        data = [obs.model_dump() for obs in cast(list[Obsplan], result)]

    # Convert schedule to the requested format
    fmt = RESPONSEFORMAT.lower()
    if fmt in ("json", "application/json"):
        # Return JSON (default behavior)
        return Response(
            content=json.dumps(data), media_type="application/json"
        )

    elif fmt in ("votable", "application/x-votable+xml", "text/xml"):
        # Convert to VOTable using astropy
        if data:
            astro_table = Table(rows=data)
        else:
            astro_table = Table()
        votable = from_table(astro_table)
        buffer = io.BytesIO()
        writeto(votable, buffer)
        return Response(
            content=buffer.getvalue(),
            media_type="application/x-votable+xml",
        )

    elif fmt in ("csv", "text/csv"):
        # Convert to CSV
        if data:
            astro_table = Table(rows=data)
        else:
            astro_table = Table()
        csv_buffer = io.StringIO()
        astro_table.write(csv_buffer, format="csv")
        return Response(content=csv_buffer.getvalue(), media_type="text/csv")

    else:
        raise HTTPException(
            status_code=415,
            detail=f"Unsupported Media Type {RESPONSEFORMAT}",
        )


@external_router.get(
    "/skymap",
    description="Interactive all-sky map of the observing schedule.",
    response_class=HTMLResponse,
    summary="Sky Map",
    include_in_schema=False,
)
async def get_skymap(
    start: str = Query("now", description="Start time ('now' or ISO/MJD)"),
    time: int = Query(24, description="Hours of schedule lookahead"),
    logger: BoundLogger = Depends(logger_dependency),
) -> HTMLResponse:
    logger.info(f"Skymap requested for time: {time}, start {start}")
    try:
        dbhelp = await DbHelpProvider.getHelper()
        if start and start.lower() != "now":
            t = Time.now() - TimeDelta("1440min")
            success = False
            try:
                t = Time(start, format="iso", scale="utc")
                success = True
            except Exception:
                pass
            try:
                t = Time(float(start), format="mjd")
                success = True
            except Exception:
                pass
            logger.info(f"Converted time: {success} - Using start time: {t}")
            schedule = cast(
                list[Obsplan], await dbhelp.get_schedule(time, start=t)
            )
        else:
            schedule = cast(list[Obsplan], await dbhelp.get_schedule(time))
        html = make_sky_html(
            schedule,
            start_val=start,
            time_val=time,
            path_prefix=config.path_prefix,
        )
        return HTMLResponse(content=html)
    except Exception as e:
        logger.error(f"Error generating skymap: {e}")
        error_html = (
            "<!DOCTYPE html><html><head><title>Error</title></head><body>"
            "<h1>Error loading skymap</h1>"
            "<p>Unable to generate the sky map. Please try again later.</p>"
            f"<p><strong>Details: </strong> {e}</p>"
            f'<p><a href="{config.path_prefix}/skymap">Retry</a></p>'
            "</body></html>"
        )
        return HTMLResponse(content=error_html, status_code=500)


@external_router.get(
    "/exposures",
    description="Return exposures from ConsDB.",
    response_model=list[Exposure],
    response_model_exclude_none=True,
    summary="Exposures",
)
async def get_exposures(
    RESPONSEFORMAT: str = Query(
        "json",
        description="Response format: json, application/json, "
        "votable, application/x-votable+xml, text/xml, csv, text/csv",
    ),
    columns: str = Query(
        "",
        description="Comma-separated list of columns to return. "
        "Default (empty) returns all columns.",
    ),
    predicate: str = Query(
        "",
        description='Filter predicate, e.g. "s_ra > 100 AND s_dec < 50" or '
        "\"band = 'r'\". Supports =, !=, <, >, <=, >=, LIKE, AND, OR.",
    ),
    limit: int = Query(1000, description="Maximum number of rows to return"),
    logger: BoundLogger = Depends(logger_dependency),
) -> Response:
    logger.info(f"Exposures requested with predicate: {predicate}")

    # Parse and validate columns parameter
    requested_cols: list[str] = []
    if columns:
        requested_cols = [c.strip() for c in columns.split(",") if c.strip()]
        try:
            validate_exposure_columns(requested_cols)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    # Validate predicate
    validated_predicate: str | None = None
    if predicate:
        try:
            validated_predicate = validate_exposure_predicate(predicate)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    consdbhelp = await ConsDbHelpProvider.getHelper()
    result = await consdbhelp.get_exposures(
        columns=requested_cols or None,
        predicate=validated_predicate,
        limit=limit,
    )

    # Convert to dicts - result is already dicts if columns specified
    if requested_cols:
        data: list[dict[str, Any]] = cast(list[dict[str, Any]], result)
    else:
        data = [exp.model_dump() for exp in cast(list[Exposure], result)]

    # Convert to the requested format
    fmt = RESPONSEFORMAT.lower()
    if fmt in ("json", "application/json"):
        return Response(
            content=json.dumps(data), media_type="application/json"
        )

    elif fmt in ("votable", "application/x-votable+xml", "text/xml"):
        if data:
            astro_table = Table(rows=data)
        else:
            astro_table = Table()
        votable = from_table(astro_table)
        buffer = io.BytesIO()
        writeto(votable, buffer)
        return Response(
            content=buffer.getvalue(),
            media_type="application/x-votable+xml",
        )

    elif fmt in ("csv", "text/csv"):
        if data:
            astro_table = Table(rows=data)
        else:
            astro_table = Table()
        csv_buffer = io.StringIO()
        astro_table.write(csv_buffer, format="csv")
        return Response(content=csv_buffer.getvalue(), media_type="text/csv")

    else:
        raise HTTPException(
            status_code=415,
            detail=f"Unsupported Media Type {RESPONSEFORMAT}",
        )
