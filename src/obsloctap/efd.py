"""Helper for the efd -so it may be mocked in test."""

__all__ = ["EfdHelp", "MockEfdHelp"]

import logging
import os

from lsst_efd_client import EfdClient

from obsloctap.models import Observation

# Configure logging
log = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter(
    "%(asctime)s [%(name)-12s] %(levelname)-8s %(message)s"
)
handler.setFormatter(formatter)
log.addHandler(handler)
if "LOG_LEVEL" in os.environ:
    log.setLevel(os.environ["LOG_LEVEL"].upper())
else:
    log.setLevel("DEBUG")


class EfdHelp:
    efd_helper = None

    def __init__(self, client):
        """
        Setup with the efd influx client passed in
        :type client: EfdClient.influx_client
        """
        self.client = client

    @staticmethod
    def getHelper():
        """
        return the helper
        """
        if EfdHelp.efd_helper is None:
            if "EFD" in os.environ:
                efd = os.environ["EFD"]
                client = EfdClient(efd_name=efd)
                client.output = "dataframe"
                EfdHelp.efd_helper = EfdHelp(client=client.influx_client)
            else:
                EfdHelp.efd_helper = MockEfdHelp(None)
                logging.warning("Using MOCK EFD - EFD env not set.")
        return EfdHelp.efd_helper

    def processObsQuery(self, result) -> list[Observation]:
        """
        Process the result of the influx query.
        The observations are returned in one record with
        fields packed in arrays.

        :type result: Dataframe
        :return: List of Observations
        """
        obslist = list[Observation]()
        for t in range(0, result.numberOfTargets[0]):
            obs = Observation()
            obs.mjd = result[f"mjd{t}"][0]
            obs.ra = result[f"ra{t}"][0]
            obs.dec = result[f"decl{t}"][0]
            obs.rotSkyPos = result[f"rotSkyPos{t}"][0]
            obs.nexp = result[f"nexp{t}"][0]
        return obslist

    def get_schedule(self) -> list[Observation]:
        """Return the latest schedule item from the EFD"""
        query = (
            'SELECT * FROM "efd"."autogen"."lsst.sal.Scheduler.'
            'logevent_predictedSchedule" '
            "where numberOfTargets > 0 ORDER BY DESC LIMIT 1"
        )
        result = await self.client.query(query)
        return self.processObsQuery(result)


class MockEfdHelp(EfdHelp):
    def get_schedule(self) -> list[Observation]:
        observations = []
        obs = Observation(
            mjd="60032.194918981484",
            ra=90.90909091666666,
            dec=-74.60384434722222,
            rotSkyPos=18.33895879413964,
            nexp=3,
        )
        observations.append(obs)
        return observations
