"""Models for obsloctap."""

from typing import Optional

from pydantic import BaseModel, Field
from safir.metadata import Metadata as SafirMetadata

__all__ = ["Index", "Obsplan"]


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


class Obsplan(BaseModel):
    """Obsplan table for the schedule"""

    __tablename__ = "ObsPlan"
    # logevent_predictedSchedule.mjd
    t_planning: Optional[float] = 0  # mjd
    target_name: Optional[str] = None
    obs_id: Optional[str] = None  # dataId[’exposure’] or obsid from camera
    obs_collection: Optional[str] = None  # dataId[’collection’]?
    s_ra: Optional[float] = 0  # ra from the scheduled observation
    s_dec: Optional[float] = 0  # dec from the scheduled observation
    s_fov: Optional[int] = 3  # always 3
    s_region: Optional[str] = None
    # we could do this, though not sure we should store it
    s_resolution: Optional[float] = 0.2  # always 0.2 arcsec
    t_min: Optional[float] = 30  # dimensionRecord.timespan.start
    t_max: Optional[float] = 30  # dimensionRecord.timespan.end
    t_exptime: Optional[float] = 30  # t_max - t_min
    t_resolution: Optional[int] = 15  # always 15s
    em_min: Optional[float] = (
        0  # Start in spectral coordinates - filter low edge in meters
    )
    em_max: Optional[float] = (
        0  # Stop in spectral coordinates - filter high edge in meters
    )
    em_res_power: Optional[float] = 0
    o_ucd: Optional[str] = "phot.flux.density"  # phot.flux.density?
    pol_states: Optional[str] = None
    pol_xel: Optional[int] = 0
    facility_name: Optional[str] = "Vera C. Rubin Observatory"
    instrument_name: Optional[str] = None  # dataId[’instrument’]
    t_plan_exptime: Optional[float] = 30  # logevent_predictedSchedule.mjd
    category: Optional[str] = "Fixed"
    priority: Optional[int] = 1  # 1
    execution_status: Optional[str] = "Scheduled"
    # Scheduled, Unscheduled, Performed, Aborted
    tracking_type: Optional[str] = "Sidereal"
    rubin_rot_sky_pos: Optional[float] = (
        0  # logevent_predictedSchedule.rotSkyPos
    )
    # NOT in Obsplan
    rubin_nexp: Optional[int] = 1  # usually 1,
    # logevent_predictedSchedule.nexp NOT in Obsplan
