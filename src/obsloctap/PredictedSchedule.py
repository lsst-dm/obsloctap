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

"""Gethe 24 hour schedule
Make an API call to rubin-sim
then foirmat to obsplan and store in DB.
"""

__all__ = ["convert_predicted"]

from pandas import DataFrame

from obsloctap.models import Obsplan

COLS = [  # as in the schedule message Kafka or EFD
    "mjd",
    "ra",
    "decl",
    "exptime",
    "nexp",
    "rotSkyPos",
]
TO_COLS = [  # as in the ObPlan object
    "t_planning",
    "s_ra",
    "s_dec",
    "t_exptime",
    "rubin_nexp",
    "rubin_rot_sky_pos",
]
SINGLE_COLS = [
    "instrumentConfiguration",
    "numberOfTargets",
    "private_efdStamp",
]


def convert_predicted(msg: DataFrame) -> list[Obsplan]:
    """The predicted schedule msg which is in EFD or comes from Kakfa
    contians all the columns defined in COLS but all in one row with e.g
    s_ra_1 s_ra_2. So need to parse that out to someting usefull"""

    plan = []
    count = 0
    while f"{COLS[0]}{count}" in msg.columns:
        p = Obsplan()
        for cc in range(0, len(COLS)):
            v = msg[f"{COLS[cc]}{count}"].head().values[0]
            p.__setattr__(TO_COLS[cc], v)
            cc += 1
        p.priority = 2  # more likely than 24 hr schedule
        if p.t_planning and p.t_planning > 0:
            plan.append(p)
        count += 1
    return plan
