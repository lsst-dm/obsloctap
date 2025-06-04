import asyncio
import os
import unittest

import pandas as pd
import pytest
from pandas import DataFrame

from obsloctap.config import Configuration
from obsloctap.db import DbHelpProvider
from obsloctap.schedule24h import Schedule24
from tests.DBmock import SqliteDbHelp


class MockSchedule(Schedule24):
    def get_schedule24(self) -> DataFrame:
        visits = pd.read_pickle("tests/schedule24rs.pkl")
        return visits


class TestSchedule(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        os.environ["database_url"] = ":memory:"
        os.environ["database_schema"] = ""
        os.environ["sleeptime"] = "0.001"

    async def asyncTearDown(self) -> None:
        DbHelpProvider.clear()

    def test_schedule_format(self) -> None:
        # schedule24.pkl was direclty from sqlite files
        # schedule24rs.pkl is from the api call (see testutils)
        visits = pd.read_pickle("tests/schedule24rs.pkl")
        obs = Schedule24().format_schedule(visits)
        self.assertEqual(len(obs), len(visits), "Not all entries")
        self.assertGreaterEqual(len(obs), 10, "Not enough entries")
        self.assertGreater(obs[0].t_planning, 0, "t_planning should not be 0")
        self.assertGreater(
            obs[0].t_planning, obs[0].t_min, "t_planning should be >t_min"
        )

    @pytest.mark.asyncio
    async def test_schedule24(self) -> None:
        lite = SqliteDbHelp()
        dbhelp = await SqliteDbHelp.getSqlLite()
        await lite.setup_schema()

        obs = await dbhelp.get_schedule(0)
        self.assertEqual(len(obs), 0, " DB should be empty to start with ")

        config = Configuration()
        print(f"Sleeptime is {config.sleeptime}")
        ms = MockSchedule()
        reps = 3
        await ms.do24hs(reps)
        print(f"Executed loop {ms.count} times")
        self.assertEqual(ms.count, reps, f"Should run {reps} times ")
        obs = await dbhelp.get_schedule(0)
        self.assertGreater(len(obs), 0, " DB should have entries at end")


python_classes = "TestCase"


if __name__ == "__main__":
    ts = TestSchedule()
    if "database_url" not in os.environ:
        os.environ["database_url"] = ":memory:"
        os.environ["database_schema"] = ""
    if "sleeptime" not in os.environ:
        os.environ["sleeptime"] = "0.001"
    print("Run test_schedule24")
    runner = asyncio.Runner()
    runner.run(ts.test_schedule24())
