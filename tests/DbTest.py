# This should only run where the database is available..
from __future__ import annotations

import pytest

from obsloctap.db import DbHelpProvider, MockDbHelp
from obsloctap.models import Observation


@pytest.mark.asyncio
async def test_insert() -> None:
    dbhelp = DbHelpProvider().getHelper()
    if isinstance(dbhelp, MockDbHelp):
        print("Not running DB test - got Mock DB")
        return

    plan = Observation(
        mjd="60032.194918981484",
        ra=90.90909091666666,
        dec=-74.60384434722222,
        rotSkyPos=18.33895879413964,
        nexp=3,
    )

    count = await dbhelp.insert_obsplan([plan])
    assert 1 == count
