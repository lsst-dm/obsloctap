"""Models for obsloctap."""

from pydantic import BaseModel, Field
from safir.metadata import Metadata as SafirMetadata

__all__ = ["Index", "Observation"]


class Index(BaseModel):
    """Metadata returned by the external root URL of the application.

    Notes
    -----
    As written, this is not very useful. Add additional metadata that will be
    helpful for a user exploring the application, or replace this model with
    some other model that makes more sense to return from the application API
    root.
    """

    metadata: SafirMetadata = Field(..., title="Package metadata")


class Observation(BaseModel):
    """Single schedule observation"""

    mjd: str
    ra: float
    dec: float
    rotSkyPos: float
    nexp: int
