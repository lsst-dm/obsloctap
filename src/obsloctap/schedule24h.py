"""Gethe 24 hour schedule
Initilly goign to sqllite - later may bean API call
sqllite code directly from Lynne"""

__all__ = ["Schedule24"]

import logging
import sqlite3

import numpy as np
import pandas as pd
from astropy.time import Time, TimeDelta
from lsst.resources import ResourcePath
from pandas import DataFrame

from obsloctap.db import DbHelpProvider
from obsloctap.models import Obsplan, spectral_ranges

texp = TimeDelta("30s")
tbuffer = TimeDelta("10min")
t24h = TimeDelta("24hr")


class Schedule24:
    def __init__(self) -> None:
        """
        Setup time stamp for 24 hour plan retrieval.
        :return: None
        """
        self.last = Time.now() - t24h

    def get_schedule24(self) -> DataFrame:
        day_obs = Time(
            np.floor(Time.now().mjd - 0.5), format="mjd", scale="utc"
        ).iso[0:10]
        sim_index = "1"

        archive_uri = "s3://rubin:rubin-scheduler-prenight/opsim/"
        sim_uri = (
            ResourcePath(archive_uri)
            .join(day_obs)
            .join(sim_index)
            .join("opsim.db")
        )

        columns = [
            "fieldRA",
            "fieldDec",
            "observationStartMJD",
            "band",
            "target_name",
        ]
        query = (
            "select " + ", ".join([c for c in columns]) + " from observations"
        )
        with sim_uri.as_local() as opsim_db:
            with sqlite3.connect(opsim_db.ospath) as sim_connection:
                visits = pd.read_sql(query, sim_connection)

        return visits

    # see DMTN-263
    def format_schedule(self, visits: DataFrame) -> list[Obsplan]:
        obslist: list[Obsplan] = []
        for ind, v in visits.iterrows():
            obs = Obsplan()
            obs.priority = 2
            obs.execution_status = "Scheduled"
            obs.s_ra = v["fieldRA"]
            obs.s_dec = v["fieldDec"]
            time = Time(v["observationStartMJD"], format="mjd")
            obs.t_min = time - tbuffer
            obs.t_max = time + texp + tbuffer
            spectral_range = spectral_ranges[v["band"]]
            obs.em_min = spectral_range[0]
            obs.em_max = spectral_range[1]
            obslist.append(obs)
        return obslist

    async def get_update_schedule24(self) -> int:
        """Get 24 hor schedule and put it in the obsplan table -
        only do if not done in last 24h s or we are new
        manage updates to come

        Returns number of rows inserted"""
        if (Time.now() - self.last) > t24h:
            logging.info("Getting 24hr schedule")
            visits = self.get_schedule24()
            obsplan = self.format_schedule(visits)
            dbhelp = await DbHelpProvider.getHelper()
            return await dbhelp.insert_obsplan(obsplan)
        return 0
