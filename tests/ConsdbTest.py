import os
import pickle
import unittest

import pytest
from sqlalchemy import text

from obsloctap.consdbhelp import ConsDbHelp, ConsDbHelpProvider
from tests.DBmock import SqliteDbHelp


@pytest.mark.asyncio
class TestConsdb(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        os.environ["consdb_url"] = ":memory:"
        os.environ["consdb_schema"] = ""

    async def asyncTearDown(self) -> None:
        os.environ["consdb_url"] = ""
        ConsDbHelpProvider.consdb_helper = None

    @staticmethod
    async def setup_db() -> ConsDbHelp:
        os.environ["consdb_url"] = ":memory:"
        os.environ["consdb_schema"] = ""
        os.environ["database_url"] = ":memory:"
        os.environ["database_schema"] = ""

        lite = SqliteDbHelp()
        dbhelp = await SqliteDbHelp.getConsDbMock()
        await lite.setup_consdb_schema()
        return dbhelp

    async def test_process(self) -> None:
        # Load data from pickle file for testing
        with open("tests/consdb.pkl", "rb") as f:
            exposures = pickle.load(f)
        cdbh: ConsDbHelp = await ConsDbHelpProvider.getHelper()
        exps = cdbh.process(exposures)
        count = len(exps)
        print(f"Loaded {count} exps from pickle of {len(exposures)}")
        # there are some pinholes which were marked on sky with no RA DEC
        self.assertEqual(967, len(exps))

    async def test_get_exposures_between(self) -> None:
        # Example test: should return data loaded from consdb.pkl
        helper = await TestConsdb.setup_db()
        s = helper.get_session()
        res = await s.execute(text("select count(*) from exposure"))
        print(res.fetchone())
        # mjd
        # start = 60858.98263978243
        # skip 2
        start = 60858.985865170325
        end = 60859.98263978243
        exposures = await helper.get_exposures_between(start, end)
        assert isinstance(exposures, list)
        # two less than we loaded
        self.assertEqual(534, len(exposures))
