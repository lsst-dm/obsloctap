# This should only run where the database is available.
# It will only run of the database_url env is set.

from __future__ import annotations

import os
import unittest

import pandas as pd
import pytest
from astropy.time import Time

from obsloctap.db import OBSPLAN_FIELDS, DbHelp
from obsloctap.models import Obsplan
from obsloctap.schedule24h import Schedule24
from tests.ConsdbTest import TestConsdb
from tests.DBmock import SqliteDbHelp


class TestDB(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        os.environ["database_url"] = ":memory:"
        os.environ["database_schema"] = ""

    async def asyncTearDown(self) -> None:
        os.environ["database_url"] = ""

    @staticmethod
    async def setup_db() -> DbHelp:
        os.environ["database_url"] = ":memory:"
        os.environ["database_schema"] = ""
        lite = SqliteDbHelp()
        dbhelp = await SqliteDbHelp.getSqlLite()
        await lite.setup_schema()
        return dbhelp

    @pytest.mark.asyncio
    async def test_insert(self) -> None:
        dbhelp = await TestDB.setup_db()

        plan = Obsplan()
        now: float = Time.now().to_value("mjd")
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
        plans = await dbhelp.get_schedule(time=0)

        await dbhelp.tidyup(now)
        assert len(plans) >= 1

        outplan = plans[len(plans) - 1]
        for key in OBSPLAN_FIELDS:
            assert plan.__dict__[key] == outplan.__dict__[key]

    @pytest.mark.asyncio
    async def test_insert_schedule(self) -> None:
        dbhelp = await TestDB.setup_db()
        visits = pd.read_pickle("tests/schedule24rs.pkl")
        obsplan = Schedule24().format_schedule(visits)
        await dbhelp.remove_flag(obsplan)
        await dbhelp.mark_old_obs()
        count = await dbhelp.insert_obsplan(obsplan)
        assert count == len(visits)
        plans = await dbhelp.get_schedule(
            time=0
        )  # should get all in this case
        self.assertEqual(
            len(plans),
            len(visits),
            "Did not get correct number of vists from get schedule",
        )

        plans_time = await dbhelp.get_schedule(
            time=48, start=Time(obsplan[-2].t_planning, format="mjd")
        )  # should get 1 less than all of them ..
        self.assertEqual(
            len(plans_time),
            len(visits) - 1,
            "Did not get correct number of vists from get schedule",
        )

        # just check one round trips
        outplan = plans[len(plans) - 1]
        for plan in obsplan:  # find the original
            if plan.t_planning == outplan.t_planning:
                break
        for key in OBSPLAN_FIELDS:
            assert plan.__dict__[key] == outplan.__dict__[key]

    @pytest.mark.asyncio
    async def test_update_entries(self) -> None:
        dbhelp = await TestDB.setup_db()
        visits = pd.read_pickle("tests/schedule24rs.pkl")
        obsplan = Schedule24().format_schedule(visits)
        await dbhelp.insert_obsplan(obsplan)
        oldest = await dbhelp.find_oldest_obs()
        # now we have visits get exposures

        helper = await TestConsdb.setup_db()
        start = 60858.98263978243
        assert oldest < start

        # skip 2
        end = 60859.98263978243
        exposures = await helper.get_exposures_between(start, end)
        res = await dbhelp.update_entries(exposures)
        assert res == 0  # no overlap here
        # lets modify N entrues to get them update
        N = 4
        for i in range(1, N):
            exposures[i].s_ra = obsplan[i].s_ra
            exposures[i].s_dec = obsplan[i].s_dec
            exposures[i].obs_start_mjd = obsplan[i].t_planning
            exposures[i].obs_end_mjd = (
                obsplan[i].t_planning + obsplan[i].t_exptime
            )

        res = await dbhelp.update_entries(exposures)
        assert res == N  # no overlap here
