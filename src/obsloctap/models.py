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

"""Models for obsloctap."""

from pydantic import BaseModel, Field
from safir.metadata import Metadata as SafirMetadata

__all__ = ["Index", "Obsplan", "spectral_ranges"]


spectral_ranges = {
    "u": [3.3e-07, 4e-07],
    "u~nd": [3.3e-07, 4e-07],
    "g": [4.02e-07, 5.52e-07],
    "g~nd": [4.02e-07, 5.52e-07],
    "r": [5.52e-07, 6.91e-07],
    "r~nd": [5.52e-07, 6.91e-07],
    "i": [6.91e-07, 8.18e-07],
    "i~nd": [6.91e-07, 8.18e-07],
    "z": [8.18e-07, 9.22e-07],
    "z~nd": [8.18e-07, 9.22e-07],
    "y": [9.22e-07, 1.06e-06],
    "y~nd": [9.22e-07, 1.06e-06],
    "other:pinhole": [3.8e-07, 7e-07],
    "ot~": [3.8e-07, 7e-07],
}


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
    t_planning: float = 0.0  # mjd
    target_name: str = ""
    obs_id: str = ""  # dataId[’exposure’] or obsid from camera
    obs_collection: str = ""  # dataId[’collection’]?
    s_ra: float = 0.0  # ra from the scheduled observation
    s_dec: float = 0.0  # dec from the scheduled observation
    s_fov: int = 3  # always 3
    s_region: str = ""
    # we could do this, though not sure we should store it
    s_resolution: float = 0.2  # always 0.2 arcsec
    t_min: float = 30.0  # dimensionRecord.timespan.start
    t_max: float = 30.0  # dimensionRecord.timespan.end
    t_exptime: float = 30.0  # t_max - t_min
    t_resolution: int = 15  # always 15s
    em_min: float = (
        0.0  # Start in spectral coordinates - filter low edge in meters
    )
    em_max: float = (
        0.0  # Stop in spectral coordinates - filter high edge in meters
    )
    em_res_power: float = 0.0
    o_ucd: str = "phot.flux.density"  # phot.flux.density?
    pol_states: str = ""
    pol_xel: int = 0
    facility_name: str = "Vera C. Rubin Observatory"
    instrument_name: str = "LSSTCam"  # dataId[’instrument’]
    t_plan_exptime: float = 30.0  # logevent_predictedSchedule.mjd
    category: str = "Fixed"
    priority: int = 1  # 1
    execution_status: str = "Scheduled"
    # Scheduled, Unscheduled, Performed, Aborted
    tracking_type: str = "Sidereal"
    rubin_rot_sky_pos: float = 0.0  # logevent_predictedSchedule.rotSkyPos
    # NOT in Obsplan
    rubin_nexp: int = 1  # usually 1,
    # logevent_predictedSchedule.nexp NOT in Obsplan


class Exposure(BaseModel):
    """this is to hold the consdb results - its not all cols"""

    exposure_id: int
    obs_start_mjd: float
    obs_end_mjd: float
    band: str
    physical_filter: str
    s_ra: float
    s_dec: float
    target_name: str
    science_program: str
    scheduler_note: str
    sky_rotation: float
