import unittest

import pandas as pd

from obsloctap.schedule24h import Schedule24


class TestSchedule(unittest.TestCase):
    def test_schedule_format(self) -> None:
        # schedule24.pkl was direclty from sqlite files
        # schedule24rs.pkl is from the api call (see testutils)
        visits = pd.read_pickle("tests/schedule24rs.pkl")
        obs = Schedule24().format_schedule(visits)
        self.assertEqual(len(obs), len(visits), "Not all entries")
        self.assertGreaterEqual(len(obs), 10, "Not enough entries")
        self.assertGreater(obs[0].t_planning, 0, "t_planning should not be 0")


python_classes = "TestCase"
