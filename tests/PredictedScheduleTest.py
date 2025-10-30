import os
import pickle
import unittest

import pandas as pd
import pytest

from obsloctap.config import config
from obsloctap.consdbhelp import ConsDbHelpProvider
from obsloctap.consumekafka import (
    convert_predicted,
    convert_predicted_kafka,
    unpack_value,
)
from obsloctap.db import DbHelpProvider
from obsloctap.schedule24h import Schedule24
from obsloctap.ScheduleLoop import do_exp_updates
from tests.ConsdbTest import TestConsdb
from tests.DBmock import SqliteDbHelp


class TestSchedule(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        os.environ["database_url"] = ":memory:"
        os.environ["database_schema"] = ""
        os.environ["exp_sleeptime"] = "0.001"
        os.environ["consdb_url"] = ":memory:"
        os.environ["consdb_schema"] = ""

    async def asyncTearDown(self) -> None:
        DbHelpProvider.clear()
        os.environ["consdb_url"] = ""
        ConsDbHelpProvider.consdb_helper = None

    async def test_kafka_schema(self) -> None:
        schema = {}
        with open("tests/schema2191.pkl", "rb") as s:
            schema = pickle.load(s)
        msg = None
        # this is a binary message dumped form kafka
        with open("tests/predicted_message.pkl", "rb") as f:
            msg = pickle.load(f)
        msg_dict: dict = unpack_value(msg.value, schema)

        plan = convert_predicted_kafka(msg_dict)
        # num targets is 4 bu only one has time
        self.assertEqual(
            len(plan),
            1,  # msg_dict["numberOfTargets"],
            "Incorrect number of plan elements",
        )

    async def test_kafka(self) -> None:
        # Load data from pickle file for testing
        with open("tests/predicted_kafka.pkl", "rb") as f:
            predicted = pickle.load(f)
        obsplan = convert_predicted_kafka(predicted)
        count = len(obsplan)
        assert count == 33  # the rest are 0 time or None.
        lite = SqliteDbHelp()
        dbhelp = await SqliteDbHelp.getSqlLite()
        await lite.setup_schema()
        entries = await dbhelp.insert_obsplan(obsplan)
        self.assertEqual(count, entries, " not all plans entered ")

    def test_convert(self) -> None:
        # Load data from pickle file for testing
        with open("tests/predicted.pkl", "rb") as f:
            predicted = pickle.load(f)
        predicted.to_parquet("predicted.parquet")
        plan = convert_predicted(predicted)
        count = len(plan)
        assert count == 53  # the rest are 0 time or None.
        print(f"Loaded {count} from pickle.")

    def test_msg(self) -> None:
        # schema = get_schema()
        with open("tests/message_mt.pkl", "rb") as f:
            msg = pickle.load(f)
        assert msg
        with open("tests/schema2191.pkl", "rb") as s:
            schema = pickle.load(s)
        print(f"testing kafka - {msg.timestamp} " f"index: {msg.key}")
        rec = unpack_value(msg.value, schema)
        self.assertEqual(rec["salIndex"], config.salIndex)
        plan = convert_predicted_kafka(rec)
        self.assertEqual(19, len(plan), " Seem to not get all lines")

    @pytest.mark.asyncio
    async def test_exp_updates(self) -> None:
        lite = SqliteDbHelp()
        dbhelp = await SqliteDbHelp.getSqlLite()
        await lite.setup_schema()
        visits = pd.read_pickle("tests/schedule24rs.pkl")
        obsplan = Schedule24().format_schedule(visits)
        entries = await dbhelp.insert_obsplan(obsplan)
        obs = await dbhelp.get_schedule(0)
        self.assertEqual(len(obs), entries, " DB should have content ")

        await TestConsdb.setup_db()

        print(f"Sleeptime is {config.exp_sleeptime}")
        reps = 3
        count = await do_exp_updates(reps)
        self.assertEqual(count, reps, f"Should run {reps} times ")
