import asyncio
import pickle
import struct
import sys

from obsloctap.consdbhelp import ConsDbHelp, ConsDbHelpProvider
from obsloctap.consumekafka import get_consumer, get_schema, unpack_value
from obsloctap.schedule24h import Schedule24
from tests.ScheduleTest import MockSchedule

default_fn = "schedule24rs.pkl"
consdb_fn = "consdb60858.pkl"
msg_fn = "message_mt.pkl"
schema_fn = "schema2191.pkl"
# Environment variables from deployment


async def dump_msg() -> None:
    consumer = get_consumer()
    print(f"Starting consumer {consumer} ")
    await consumer.start()
    async for msg in consumer:
        value = msg.value  # bytes from Kafka message
        magic = value[0]
        assert magic == 0, "Not Confluent Avro wire format"
        schema_id = struct.unpack(">I", value[1:5])[0]
        schema = {}
        with open("tests/schema2191.pkl", "rb") as s:
            schema = pickle.load(s)
        try:
            schema = get_schema(schema_id)
        except Exception:
            print(f"failed to get schems {Exception}")

        msg_dict = unpack_value(value, schema)
        if msg_dict["salIndex"] == 1:
            with open(f"schema{schema_id}.pkl", "wb") as f:
                pickle.dump(schema, f)
            with open(msg_fn, "wb") as f:
                pickle.dump(msg, f)
            break
        else:
            print(f"Ignoring message {msg_dict['salIndex']}")
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
    done = False
    if "msg" in sys.argv:
        asyncio.run(dump_msg())
        done = True
    elif "consdb" in sys.argv:
        asyncio.run(store_consdb_file())
        done = True
    elif not done:
        store_schedule_file()
