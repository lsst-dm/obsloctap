# This should only run where the database is available.
# It will only run of the database_url env is set.

from __future__ import annotations

import os
import pickle
import unittest

import pandas as pd
import pytest
from astropy.time import Time

from obsloctap.consdbhelp import ConsDbHelp, ConsDbHelpProvider
from obsloctap.db import OBSPLAN_FIELDS, DbHelp
from obsloctap.models import Obsplan, spectral_ranges
from obsloctap.schedule24h import Schedule24
from tests.ConsdbTest import TestConsdb, consdbendless2, consdbstarttime
from tests.DBmock import SqliteDbHelp


class TestDB(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        os.environ["database_url"] = ":memory:"
        os.environ["database_schema"] = ""
        os.environ["consdb_url"] = ":memory:"
        os.environ["consdb_schema"] = ""

    async def asyncTearDown(self) -> None:
        os.environ["database_url"] = ""
        os.environ["consdb_url"] = ""

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
        visits = pd.read_pickle("tests/data/schedule24rs.pkl")
        obsplan = Schedule24().format_schedule(visits)
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
            "Did not get correct number of visits from get schedule",
        )

        # just check one round trips
        outplan = plans[len(plans) - 1]
        for plan in obsplan:  # find the original
            if plan.t_planning == outplan.t_planning:
                break
        for key in OBSPLAN_FIELDS:
            assert plan.__dict__[key] == outplan.__dict__[key]

        # seeing duplicates so running the same plan again ..
        count = await dbhelp.insert_obsplan(obsplan)
        self.assertEqual(0, count, " These should all be rejected")
        plans = await dbhelp.get_schedule(
            time=0
        )  # should get all in this case
        self.assertEqual(
            len(plans),
            len(visits),
            "Second run: incorrect number of visits from get schedule",
        )

    @pytest.mark.asyncio
    async def test_update_entries(self) -> None:
        dbhelp = await TestDB.setup_db()
        visits = pd.read_pickle("tests/data/schedule24rs.pkl")
        obsplan = Schedule24().format_schedule(visits)
        await dbhelp.insert_obsplan(obsplan)
        oldest = await dbhelp.find_oldest_plan()
        # now we have visits get exposures
        # but test negate
        fillin = await dbhelp.find_oldest_plan(negate=True)
        self.assertEqual(0, fillin, " Nothign is observed ")
        helper = await TestConsdb.setup_db()
        start = consdbstarttime
        first = obsplan[-1].t_planning  # should be the oldest
        last = obsplan[0].t_planning  # should be the newest
        self.assertGreater(last, first, "Order of the plans is wrong ?")
        self.assertEqual(oldest, first, "Expected to find the first plan")
        assert oldest < start

        plans = len(await dbhelp.get_schedule(time=0))
        # this should get all ..
        # having some duplicates so need to make sure
        # Updates are only updating

        # skip 2
        end = consdbendless2
        exposures = await helper.get_exposures_between(start, end)
        noexps = len(exposures)
        sess = dbhelp.get_session()
        updated, inserted = await dbhelp.update_insert_exposures(
            exposures, session_touse=sess
        )
        await sess.commit()
        assert updated == 0  # no overlap here - no updates
        # but all exposures are added/inserted
        assert inserted == noexps

        plans2 = await dbhelp.get_schedule(time=0)
        cplans2 = len(plans2)
        assert cplans2 == plans + noexps

        # in the test session the above inserts seem
        # to dissappear after the query
        plans2 = await dbhelp.get_schedule(time=0)
        cplans2 = len(plans2)
        assert cplans2 == plans + noexps

        # exposures not matching were added as observations above
        # test sessions seem to work very differently to real ones
        # so passing as session here
        updated, inserted = await dbhelp.update_insert_exposures(
            exposures, session_touse=sess
        )
        sess.commit()
        sess.close()
        # so all of these should be updated
        self.assertEqual(updated, noexps)  # should have updated the exposures
        plans2 = await dbhelp.get_schedule(time=0)
        self.assertEqual(len(plans2), plans + noexps)
        updated = await dbhelp.mark_not_observed(
            [plans2[1].t_planning, plans2[2].t_planning]
        )
        self.assertEqual(2, updated, " should have marked 2 observation")
        fillin = await dbhelp.find_oldest_plan(negate=True)
        self.assertGreater(fillin, 0, " Somethign should have been  observed ")
        # is it the oldest ?  60858.98263978243
        self.assertLessEqual(fillin, 60858.98263978243, "Not  old enough plan")
        # just to check that sql works

    @pytest.mark.asyncio
    async def test_insert_exposure(self) -> None:
        dbhelp = await TestDB.setup_db()
        # Load exposure from pickle file for testing
        plans = len(await dbhelp.get_schedule(time=0))
        assert plans == 0  # should be empty
        with open("tests/data/consdb60852.pkl", "rb") as f:
            exposures = pickle.load(f)
        cdbh: ConsDbHelp = await ConsDbHelpProvider.getHelper()
        exps = cdbh.process(exposures)

        exp = exps[0]
        await dbhelp.insert_exposure(exp, dbhelp.get_session())

        exp = exps[1]
        exp.band = None  # test this
        await dbhelp.insert_exposure(exp, dbhelp.get_session())
        plans2 = len(await dbhelp.get_schedule(time=0))
        assert plans2 == plans + 2

        exp.band = "none"  # test this
        await dbhelp.insert_exposure(exp, dbhelp.get_session())
        plans2 = len(await dbhelp.get_schedule(time=0))

    @pytest.mark.asyncio
    async def test_find_by_obs_id(self) -> None:
        dbhelp = await TestDB.setup_db()
        visits = pd.read_pickle("tests/data/schedule24rs.pkl")
        obsplan = Schedule24().format_schedule(visits)
        await dbhelp.insert_obsplan(obsplan)

        # Get the obs_id from the first obsplan entry
        obs_id = obsplan[0].obs_id
        # Find the entry by obs_id
        results = await dbhelp.find_by_obs_id(obs_id)

        # Should find 44 matched - obs_id is not unique inthe plan
        self.assertGreater(len(results), 0, "should at least have 1 match")
        found_plan = results[0]
        # Verify that the found entry matches the original
        self.assertEqual(found_plan.obs_id, obs_id)

        # Test with a non-existent obs_id
        results = await dbhelp.find_by_obs_id("NONEXISTENT_ID")
        self.assertEqual(
            len(results), 0, "Should find no entries for invalid obs_id"
        )

    @pytest.mark.asyncio
    async def test_update_insert_nextVisit(self) -> None:
        dbhelp = await TestDB.setup_db()
        visits = pd.read_pickle("tests/data/schedule24rs.pkl")
        obsplan = Schedule24().format_schedule(visits)
        await dbhelp.insert_obsplan(obsplan)

        # Get the first plan to use as a reference for matching
        first_plan = obsplan[0]

        # Case 1: Create a nextVisit that matches the first plan
        # (same s_ra, s_dec, t_planning in range [t_min, t_max])
        next_visit_match = Obsplan()
        next_visit_match.s_ra = first_plan.s_ra
        next_visit_match.s_dec = first_plan.s_dec
        next_visit_match.t_planning = (
            first_plan.t_min + (first_plan.t_max - first_plan.t_min) / 2
        )
        next_visit_match.target_name = "NextVisit_Match"
        next_visit_match.em_min = spectral_ranges["g"][0]
        next_visit_match.em_max = spectral_ranges["g"][1]
        next_visit_match.priority = 1
        next_visit_match.rubin_rot_sky_pos = 0.0
        next_visit_match.obs_id = "NV-UPDATE"

        # This should update the existing plan (return 0)
        result = await dbhelp.update_insert_nextVisit(next_visit_match)
        self.assertEqual(0, result, "Should update existing plan")

        # Case 2: Create a nextVisit that doesn't match any existing plan
        next_visit_new = Obsplan()
        next_visit_new.s_ra = 180.0
        next_visit_new.s_dec = 0.0
        next_visit_new.t_planning = 61000.0
        next_visit_new.target_name = "NextVisit_New"
        next_visit_new.em_min = spectral_ranges["r"][0]
        next_visit_new.em_max = spectral_ranges["r"][1]
        next_visit_match.priority = 1
        next_visit_new.priority = 1
        next_visit_new.rubin_rot_sky_pos = 45.0
        next_visit_new.obs_id = "NV_NEW_001"

        # This should insert a new plan (return 1)
        result = await dbhelp.update_insert_nextVisit(next_visit_new)
        self.assertEqual(1, result, "Should insert new plan")

        # Verify we have the expected number of plans
        plans = await dbhelp.get_schedule(time=0)
        self.assertEqual(
            len(plans),
            len(obsplan) + 1,
            "Should have original plans plus one new insert",
        )

    @pytest.mark.asyncio
    async def test_updateif(self) -> None:
        a = Obsplan()
        b = Obsplan()

        a.t_planning = 505.4
        a.target_name = " Test Targ"

        dbhelp = await TestDB.setup_db()

        n = await dbhelp.update_planif(a, b)

        self.assertEqual(a.t_planning, b.t_planning)
        self.assertEqual(a.target_name, n.target_name)
