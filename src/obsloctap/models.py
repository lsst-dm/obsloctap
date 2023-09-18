"""Models for obsloctap."""

from enum import Enum

from pydantic import BaseModel, Field
from safir.metadata import Metadata as SafirMetadata
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

__all__ = ["Index", "Observation", "Obsplan", "ExecStatus"]


class SqlBase(DeclarativeBase):
    pass


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


class ExecStatus(Enum):
    """Status of the execution of an observation"""

    Scheduled = 1
    Unscheduled = 2
    Performed = 3
    Aborted = 4


class Obsplan(SqlBase):
    """Obsplan table for the schedule"""

    def __init__(self) -> None:
        super(Obsplan, self).__init__()
        self.s_fov = 3
        self.s_resolution = 0.2
        self.t_resolution = 15
        self.o_ucd = "phot.flux.density"
        self.facility_name = "Vera C. Rubin Observatory"
        self.priority = 1
        self.tracking_type = "sidereal"

    __tablename__ = "obsplan"
    t_planning: Mapped[str]  # logevent_predictedSchedule.mjd
    target_name: Mapped[str]  # this will probalby always be empty
    obs_id: Mapped[int] = mapped_column(
        primary_key=True
    )  # dataId[’exposure’] or obsid from camera
    obs_collection: Mapped[str]  # dataId[’collection’]?
    s_ra: Mapped[float]  # ra from the scheduled observation
    s_dec: Mapped[float]  # dec from the scheduled observation
    s_fov: Mapped[int]  # always 3
    s_region: Mapped[
        str
    ]  # we could do this, though not sure we should store it
    s_resolution: Mapped[float]  # always 0.2 arcsec
    t_min: Mapped[float]  # dimensionRecord.timespan.start
    t_max: Mapped[float]  # dimensionRecord.timespan.end
    t_exptime: Mapped[float]  # t_max - t_min
    t_resolution: Mapped[int]  # always 15s
    em_min: Mapped[
        float
    ]  # Start in spectral coordinates - filter low edge in meters
    em_max: Mapped[
        float
    ]  # Stop in spectral coordinates - filter high edge in meters
    em_res_power: Mapped[float]
    o_ucd: Mapped[str]  # phot.flux.density?
    pol_states: Mapped[str]  # NULL
    pol_xel: Mapped[int]  # 0
    facility_name: Mapped[str]  # Vera C. Rubin Observatory
    instrument_name: Mapped[str]  # dataId[’instrument’]
    t_plan_exptime: Mapped[str]  # logevent_predictedSchedule.mjd
    category: Mapped[str]  # Fixed
    priority: Mapped[int]  # 1
    execution_status: Mapped[
        ExecStatus
    ]  # Scheduled, Unscheduled, Performed, Aborted
    tracking_type: Mapped[str]  # Sidereal
    rubin_rot_sky_pos: Mapped[
        float
    ]  # logevent_predictedSchedule.rotSkyPos NOT in Obsplan
    rubin_nexp: Mapped[
        int
    ]  # usually 1, logevent_predictedSchedule.nexp NOT in Obsplan
