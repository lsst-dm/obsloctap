"""Helper for the efd -so it may be mocked in test."""

__all__ = ["EfdHelp", "EfdHelpProvider"]

import logging
import os

from lsst_efd_client import EfdClient
from pandas import DataFrame

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
    def __init__(self, client: EfdClient) -> None:
        """
        Setup helper with the efd influx client passed in.

        Parameters
        ----------
        client : EfdClient

        :return: None
        """
        self.client = client

    def processObsQuery(self, result: DataFrame) -> list[Observation]:
        """
        Process the result of the influx query.
        The observations are returned in one record with
        fields packed in arrays.
        This turns them into a list of Observations

        :type self: EfdHelp
        :type result: DataFrame
        :return: list[Observation]
        """
        obslist = list[Observation]()
        for t in range(0, result.numberOfTargets[0]):
            mjd = result[f"mjd{t}"][0]
            ra = result[f"ra{t}"][0]
            dec = result[f"decl{t}"][0]
            rotSkyPos = result[f"rotSkyPos{t}"][0]
            nexp = result[f"nexp{t}"][0]
            obs = Observation(
                mjd=mjd, ra=ra, dec=dec, rotSkyPos=rotSkyPos, nexp=nexp
            )
            obslist.append(obs)
        return obslist

    async def get_schedule(self) -> list[Observation]:
        """Return the latest schedule item from the EFD"""
        query = (
            'SELECT * FROM "efd"."autogen"."lsst.sal.Scheduler.'
            'logevent_predictedSchedule" '
            "where numberOfTargets > 0 ORDER BY DESC LIMIT 1"
        )
        result = await self.client.query(query)
        return self.processObsQuery(result)


class MockEfdHelp(EfdHelp):
    async def get_schedule(self) -> list[Observation]:
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


# sort of singleton
efdHelper: EfdHelp | None = None


class EfdHelpProvider:
    @staticmethod
    def getHelper() -> EfdHelp:
        """
        :return: EfdHelp the helper
        """
        global efdHelper
        if efdHelper is None:
            if "EFD" in os.environ:
                efd = os.environ["EFD"]
                client = EfdClient(efd_name=efd)
                client.output = "dataframe"
                efdHelper = EfdHelp(client=client.influx_client)
            else:
                efdHelper = MockEfdHelp(None)
                logging.warning("Using MOCK EFD - EFD env not set.")

        return efdHelper
