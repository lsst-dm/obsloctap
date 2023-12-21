# This should only run where the database is available.
# It will only run of the database_url env is set.

from __future__ import annotations

import pytest
from pandas import Timestamp

from obsloctap.db import OBSPLAN_FIELDS, DbHelpProvider, MockDbHelp
from obsloctap.models import Obsplan


@pytest.mark.asyncio
async def test_insert() -> None:
    dbhelp = await DbHelpProvider().getHelper()
    if isinstance(dbhelp, MockDbHelp):
        print("Not running DB test - got Mock DB")
        return

    plan = Obsplan()
    now: float = Timestamp.now().to_julian_date()
    plan.t_planning = now
    plan.s_ra = 90.90909091666666
    plan.s_dec = -74.60384434722222
    plan.rubin_nexp = 3
    plan.rubin_rot_sky_pos = 18.33895879413964
    plan.obs_id = "OBS_1"
    plan.t_min = 30
    plan.t_max = 30
    plan.t_exptime = 30
    plan.instrument_name = "TESTCam"
    plan.category = "Fixed"
    plan.execution_status = "Scheduled"
    plan.priority = 1
    plan.target_name = "TEST"
    plan.obs_collection = "TEST"
    plan.s_region = "TEST"
    plan.em_res_power = 1.5
    plan.em_max = 2
    plan.em_min = 1
    plan.pol_xel = 1
    plan.pol_states = "TEST"
    plan.t_plan_exptime = 30

    count = await dbhelp.insert_obsplan([plan])
    assert 1 == count

    # now get it back
    plans = await dbhelp.get_schedule(time=100)

    await dbhelp.tidyup(now)

    assert len(plans) >= 1

    outplan = plans[len(plans) - 1]
    for key in OBSPLAN_FIELDS:
        assert plan.__dict__[key] == outplan.__dict__[key]
