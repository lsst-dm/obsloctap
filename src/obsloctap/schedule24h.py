"""Gethe 24 hour schedule
Make an API call to rubin-sim
then foirmat to obsplan and store in DB.
"""

__all__ = ["Schedule24"]

import asyncio
import logging
from operator import attrgetter

import structlog
from astropy.time import Time, TimeDelta
from pandas import DataFrame
from rubin_sim import __version__ as rubin_sim_version
from rubin_sim.sim_archive import sim_archive

from obsloctap.db import DbHelpProvider
from obsloctap.models import Obsplan, spectral_ranges

texp = TimeDelta("30s")
tbuffer = TimeDelta("10min")
t24h = TimeDelta("24hr")

# Configure logging
log = structlog.getLogger(__name__)


class Schedule24:
    def __init__(self) -> None:
        """
        Setup time stamp for 24 hour plan retrieval.
        :return: None
        """
        self.last = Time.now() - t24h

    def get_schedule24(self) -> DataFrame:
        """
        Get he 24 schedule form the rubin_sim api call
        Need these:
           os.environ["LSST_DISABLE_BUCKET_VALIDATION"] = "1"
           os.environ["S3_ENDPOINT_URL"] = "https://s3dfrgw.slac.stanford.edu/"
        Returns   DataFrame of schedule entries
        -------
        """
        logging.info(f"Using rubin_sim {rubin_sim_version}")

        try:
            visits = sim_archive.fetch_obsloctap_visits(nights=2)
        except TypeError:
            logging.info("Dropping to 1 night")
            visits = sim_archive.fetch_obsloctap_visits(nights=1)
        logging.info(f"Got {len(visits)} for 24 hour schedule")
        if type(visits) is not DataFrame:
            visits = DataFrame(visits)
        return visits

    # see DMTN-263
    def format_schedule(self, visits: DataFrame) -> list[Obsplan]:
        """take the datframe from rubin sim and turn it into
        a sorted list of Obsplan objects"""
        obslist: list[Obsplan] = []
        for ind, v in visits.iterrows():
            obs = Obsplan()
            obs.target_name = v["target_name"]
            obs.obs_id = v["target_name"]
            obs.priority = 2
            obs.execution_status = "Scheduled"
            obs.s_ra = v["fieldRA"]
            obs.s_dec = v["fieldDec"]
            time = Time(v["observationStartMJD"], format="mjd")
            obs.t_planning = v["observationStartMJD"]
            obs.t_min = time - tbuffer
            obs.t_max = time + texp + tbuffer
            spectral_range = spectral_ranges[v["band"]]
            obs.em_min = spectral_range[0]
            obs.em_max = spectral_range[1]
            obslist.append(obs)
        obslist.sort(key=attrgetter("t_planning"))
        log.info(
            f"Obsplan schedule from {obslist[0].t_planning} to "
            f"{obslist[-1].t_planning}"
        )
        return obslist

    async def get_update_schedule24(self) -> int:
        """Get 24 hor schedule and put it in the obsplan table -
        manage updates to come

        Returns number of rows inserted"""
        logging.info("Getting 24hr schedule")
        visits = self.get_schedule24()
        obsplan = self.format_schedule(visits)
        dbhelp = await DbHelpProvider.getHelper()
        await dbhelp.remove_flag(obsplan)
        await dbhelp.mark_old_obs()
        return await dbhelp.insert_obsplan(obsplan)

    @staticmethod
    async def do24hs() -> None:
        """this will get 24h schedule then sleep for 12hrs"""
        while True:
            await Schedule24().get_update_schedule24()
            await asyncio.sleep(12 * 60 * 60)
