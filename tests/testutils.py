import asyncio
import sys

from obsloctap.consdbhelp import ConsDbHelp, ConsDbHelpProvider
from obsloctap.schedule24h import Schedule24
from tests.ScheduleTest import MockSchedule

default_fn = "schedule24rs.pkl"
consdb_fn = "consdb.pkl"


def store_schedule_file(fn: str = default_fn) -> None:
    sp = Schedule24()
    visits = sp.get_schedule24()
    visits.to_pickle(fn)


async def store_consdb_file(fn: str = consdb_fn) -> None:
    sched = MockSchedule()
    visits = sched.get_schedule24()
    start = visits["observationStartMJD"].min()
    end = visits["observationStartMJD"].max()
    # use those times to get consdb data for a test file
    cdb: ConsDbHelp = ConsDbHelpProvider.getHelper()
    exps = await cdb.get_exposures_between(start, end)
    exps.to


if __name__ == "__main__":
    if "consdb" in sys.argv:
        asyncio.run(store_consdb_file())
    else:
        store_schedule_file()
