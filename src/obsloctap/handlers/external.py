"""Handlers for the app's external root, ``/obsloctap/``."""

from fastapi import APIRouter, Depends
from safir.dependencies.logger import logger_dependency
from safir.metadata import get_metadata
from structlog.stdlib import BoundLogger

from ..config import config
from ..models import Index, Observation

__all__ = ["get_index", "external_router"]

external_router = APIRouter()
"""FastAPI router for all external handlers."""


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
    logger.info("Request for application metadata")

    metadata = get_metadata(
        package_name="obsloctap",
        application_name=config.name,
    )
    page = Index(metadata=metadata)

    return page


@external_router.get(
    "/schedule",
    description=("Return the curent observing schedule."),
    response_model=list[Observation],
    response_model_exclude_none=True,
    summary="Observation Schedule",
)
async def get_schedule() -> list[Observation]:
    observations = []
    obs = Observation(
        mjd="60032.194918981484",
        ra=90.90909091666666,
        dec=-74.60384434722222,
        rotSkyPos=18.33895879413964,
        nexp=3,
    )
    observations.append(obs)
    return observations
