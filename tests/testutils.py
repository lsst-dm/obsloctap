import asyncio
import pickle
import sys

from obsloctap.consdbhelp import ConsDbHelp, ConsDbHelpProvider
from obsloctap.consumekafka import get_consumer, get_schema
from obsloctap.schedule24h import Schedule24
from tests.ScheduleTest import MockSchedule

default_fn = "schedule24rs.pkl"
consdb_fn = "consdb60858.pkl"
msg_fn = "message.pkl"
schema_fn = "schema.pkl"
# Environment variables from deployment


async def dump_msg() -> None:
    schema = get_schema()
    with open(schema, "wb") as f:
        pickle.dump(schema, f)

    consumer = get_consumer()
    print(f"Starting consumer {consumer} ")
    await consumer.start()
    async for msg in consumer:
        with open(msg_fn, "wb") as f:
            pickle.dump(msg, f)
        break
    await consumer.stop()


def store_schedule_file(fn: str = default_fn) -> None:
    sp = Schedule24()
    visits = sp.get_schedule24()
    visits.to_pickle(fn)


async def store_consdb_file(fn: str = consdb_fn) -> None:
    # done in notebook in the end since
    # but put here to remember
    # needs to run at SLAC
    sched = MockSchedule()
    visits = sched.get_schedule24()
    start = visits["observationStartMJD"].min()
    end = visits["observationStartMJD"].max()
    # use those times to get consdb data for a test file
    cdb: ConsDbHelp = await ConsDbHelpProvider.getHelper()
    exps = await cdb.get_exposure_rows_between(start, end)
    with open(fn, "wb") as f:
        pickle.dump(exps, f)


if __name__ == "__main__":
    if "msg" in sys.argv:
        asyncio.run(dump_msg())
        exit
    if "consdb" in sys.argv:
        asyncio.run(store_consdb_file())
        exit
    store_schedule_file()
