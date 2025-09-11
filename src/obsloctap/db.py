# This file is part of obsloctap.
#
# Developed for the Rubin Data Management System.
# This product includes software developed by the Rubin Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# Use of this source code is governed by a 3-clause BSD-style
# license that can be found in the LICENSE file.

"""Helper for the database -so it may be mocked in test."""

__all__ = ["DbHelp", "DbHelpProvider", "OBSPLAN_FIELDS"]

import os
from io import StringIO
from typing import Any, Sequence

import astropy.units as u
import structlog
from astropy.time import Time, TimeDelta
from sqlalchemy import Row, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    create_async_engine,
)

from obsloctap.models import Exposure, Obsplan, spectral_ranges

from .config import Configuration

OBSPLAN_FIELDS = [
    "t_planning",  # DOUBLE PRECISION NOT NULL,
    "target_name",  # VARCHAR,
    "obs_id",  # VARCHAR NOT NULL,
    "obs_collection",  # VARCHAR,
    "s_ra",  # DOUBLE PRECISION,
    "s_dec",  # DOUBLE PRECISION,
    "s_fov",  # DOUBLE PRECISION,
    "s_region",  # VARCHAR,
    "s_resolution",  # DOUBLE PRECISION,
    "t_min",  # DOUBLE PRECISION NOT NULL,
    "t_max",  # DOUBLE PRECISION NOT NULL,
    "t_exptime",  # DOUBLE PRECISION NOT NULL,
    "t_resolution",  # DOUBLE PRECISION,
    "em_min",  # DOUBLE PRECISION,
    "em_max",  # DOUBLE PRECISION,
    "em_res_power",  # DOUBLE PRECISION,
    "o_ucd",  # VARCHAR,
    "pol_states",  # VARCHAR,
    "pol_xel",  # INTEGER,
    "facility_name",  # VARCHAR NOT NULL,
    "instrument_name",  # VARCHAR,
    "t_plan_exptime",  # DOUBLE PRECISION,
    "category",  # VARCHAR NOT NULL,
    "priority",  # INTEGER NOT NULL,
    "execution_status",  # VARCHAR NOT NULL,
    "tracking_type",  # VARCHAR  NOT NULL,
    "rubin_rot_sky_pos",  # FLOAT,
    "rubin_nexp",  # INTEGER
]

# Configure logging
# log: BoundLogger = (Depends(logger_dependency),)
log = structlog.getLogger(__name__)
min30 = TimeDelta("30min")


class DbHelp:
    def __init__(self, engine: AsyncEngine | None) -> None:
        """
        Setup helper with the sqlclient client passed in.

        Parameters
        ----------
        engine : SqlAlchemy Engine

        :return: None
        """
        self.engine = engine
        self.schema = ""
        self.insert_fields = ",".join(OBSPLAN_FIELDS)

    def get_session(self) -> AsyncSession:
        return AsyncSession(self.engine)

    def process(self, result: Sequence[Row[Any]]) -> list[Obsplan]:
        """
        Process the result of the query to make a list of Obsplan.

        :type self: DbHelp
        :type result: Obsplan[]
        :return: list[Obsplan]
        """
        obslist = list[Obsplan]()
        for o in result:
            obs = Obsplan()
            for c, key in enumerate(OBSPLAN_FIELDS):
                setattr(obs, key, o[c])
            obslist.append(obs)
        return obslist

    async def get_schedule(
        self, time: float = 0, start: Time | None = None
    ) -> list[Obsplan]:
        """Return the latest schedule item from the DB.
         time is a number of hours from start for how much schedule to return
         if no start is provided now in UTC is assumed
        if time is zero we just take top obsplanLimit(1000) rows."""

        config = Configuration()

        whereclause = ""
        limitclause = f" limit  {config.obsplanLimit}"

        if time != 0:
            now = start if start else Time.now()
            startmjd = now.to_value("mjd")
            td = TimeDelta(time * u.hr)
            win = now + td
            window = win.to_value("mjd")

            whereclause = (
                f" where t_planning between  " f"{startmjd} AND {window}"
            )
            limitclause = ""

        statement = (
            f"select {self.insert_fields} from "
            f'{self.schema}"{Obsplan.__tablename__}"'
            f"{whereclause}"
            f" order by t_planning DESC "
            f"{limitclause}"
        )
        log.debug(f"get_schedule: {statement}")
        session = AsyncSession(self.engine)
        result = await session.execute(text(statement))
        obs = result.all()
        log.debug(f"Got scedule with {len(obs or [])} elements")
        if len(obs) == 0 and time != 0:
            log.info(f"No observations between {startmjd}" f"and {window}")
        await session.close()
        return self.process(obs)

    async def insert_obs(
        self, session: AsyncSession, observation: Obsplan
    ) -> None:
        """Insert an observation into the DB"""

        value_str = StringIO()
        for count, key in enumerate(OBSPLAN_FIELDS):
            if getattr(observation, key) is None:
                value_str.write("NULL")
            else:
                value_str.write(f"'{getattr(observation, key)}'")
            if (count + 1) < len(OBSPLAN_FIELDS):
                value_str.write(",")

        stmt = (
            f'insert into {self.schema}"{Obsplan.__tablename__}" '
            f"values ({value_str.getvalue()})"
        )
        # log.debug(stmt)
        await session.execute(text(stmt))

    async def insert_obsplan(self, observations: list[Obsplan]) -> int:
        """Insert observations into the DB -
        return the count of inserted rows."""
        session = AsyncSession(self.engine)
        for observation in observations:
            await self.insert_obs(session, observation)
        await session.commit()
        await session.close()
        log.info(
            f"Inserted and commited {len(observations or [])} Observations."
        )
        return len(observations or [])

    async def update_entries(
        self, exposures: list[Exposure], tol: float = 1
    ) -> int:
        """
        Update obsplan entries that match exposures by s_ra, s_dec,
        and obs_start_mjd in [t_min, t_max].
        tol: tolerances for matching s_ra and s_dec.
        Check it is not 'performed' if it is onlyupdate if same id.
        Returns the number of updated entries.
        """
        session = AsyncSession(self.engine)
        updated = 0
        unmatched = 0
        for exp in exposures:
            # Find matching obsplan entries
            stmt = (
                f'SELECT * FROM {self.schema}"{Obsplan.__tablename__}" '
                f"WHERE ABS(s_ra - {exp.s_ra}) < {tol} "
                f"AND ABS(s_dec - {exp.s_dec}) < {tol} "
                f"AND t_min <= {exp.obs_start_mjd} AND "
                f"t_max >= {exp.obs_start_mjd}"
            )
            result = await session.execute(text(stmt))
            matches = result.fetchall()
            done = False
            if len(matches) > 0:
                match = matches[0]
                # update execution_status to 'Performed'
                # unless it is Performed .. then we better chekc id
                # this will mean we can update to get new info from consdb
                # but not clobber it if the id is different.
                if match.execution_status != "Performed" or (
                    match.execution_status == "Performed"
                    and match.obs_id == str(exp.exposure_id)
                ):
                    await self.update_one(exp, match, session)
                    updated += 1
                    done = True

            if not done:
                unmatched += 1
                await self.insert_exposure(exp, session)

        await session.commit()
        log.info(
            f"Updated {updated}, unmatched {unmatched} of {len(exposures)}"
        )
        return updated

    async def insert_exposure(
        self, exp: Exposure, session: AsyncSession
    ) -> None:
        """put in  an obsplan line based on an exposure
        this is when consdb has an observation but it does not match
        any planned item"""
        value_str = (
            f"0, "  # t_planning
            f"'{exp.target_name}', "  # target_name
            f"'{str(exp.exposure_id)}', "  # obs_id
            f"'{exp.scheduler_note}', "  # obs_collection
            f"{exp.s_ra}, "  # s_ra
            f"{exp.s_dec}, "  # s_dec
            f"3, "  # s_fov
            f"'', "  # s_region
            f"0.2, "  # s_resolution
            f"{exp.obs_start_mjd}, "  # t_min
            f"{exp.obs_end_mjd}, "  # t_max
            f"{exp.obs_end_mjd - exp.obs_start_mjd}, "  # t_exptime
            f"15, "  # t_resolution
            f"'{spectral_ranges[exp.band][0]}', "  # em_min
            f"'{spectral_ranges[exp.band][1]}', "  # em_max
            f"0, "  # em_res_power
            f"'phot.flux.density', "  # o_ucd
            f"'', "  # pol_states
            f"0, "  # pol_xel
            f"'Vera C. Rubin Observatory', "  # facility_name
            f"'LSSTCam', "  # instrument_name
            f"0, "  # t_plan_exptime
            f"'{exp.science_program}', "  # category
            f"{1}, "  # priority
            f"'Performed', "  # execution_status
            f"'sidereal', "  # tracking_type
            f"{exp.sky_rotation}, "  # rubin_rot_sky_pos
            f"{1}"  # rubin_nexp
        )

        insert_stmt = (
            f'insert into {self.schema}"{Obsplan.__tablename__}" '
            f"values ({value_str}) "
        )
        await session.execute(text(insert_stmt))

    async def update_one(
        self, exp: Exposure, match: Row, session: AsyncSession
    ) -> None:
        """update an obsplan line based on an exposure"""
        update_stmt = (
            f'UPDATE {self.schema}"{Obsplan.__tablename__}" '
            f"SET execution_status = 'Performed', "
            f"obs_id = '{exp.exposure_id}', "
            f"target_name = '{exp.target_name}', "
            f"obs_collection = '{exp.science_program}| {exp.scheduler_note}', "
            f"em_min = '{spectral_ranges[exp.band][0]}', "
            f"em_max = '{spectral_ranges[exp.band][1]}', "
            f"t_min = {exp.obs_start_mjd}, "
            f"t_max = {exp.obs_end_mjd}, "
            f"rubin_rot_sky_pos = {exp.sky_rotation} "
            f"WHERE obs_id = '{match.obs_id}'"
        )
        await session.execute(text(update_stmt))

    async def remove_flag(
        self, observations: list[Obsplan], priority: int = 2
    ) -> int:
        """Look at the obsplan table wrt to the new schedule,
        if there is a new simlar entry remove the existing one,
        if there is a time with a different entry mark it as not observed.
        Perhaps one could also delete the not observed ones.
        Observations should be sorted on t_planning.

        Returns number of entries marked not executed"""

        if len(observations) == 0:
            return 0
        # oservations are sorted
        mint = observations[-1].t_planning
        maxt = observations[0].t_planning

        statement = (
            f"select {self.insert_fields} from "
            f'{self.schema}"{Obsplan.__tablename__}"'
            f" where t_planning between {mint} and {maxt}"
            f" order by t_planning DESC"
        )
        log.debug(f"remove_flag: {statement}")
        session = AsyncSession(self.engine)
        result = await session.execute(text(statement))
        oldobs: list[Obsplan] = self.process(result.all())
        await session.close()
        todelete: list = []
        tomark: list = []
        obscount = 0
        # loop over the old observations and match to new ones
        # both lists in descending order
        for obs in oldobs:
            notfound: bool = True
            while notfound and obscount < len(observations):
                newobs: Obsplan = observations[obscount]
                if newobs.t_min < obs.t_planning < newobs.t_max:
                    # same time frame - and this is schedule
                    # so we delete the old and put in the new
                    #
                    if obs.obs_id == newobs.obs_id:
                        # its a match so we replace it
                        todelete.append(obs.t_planning)
                        obscount += 1
                        notfound = False
                if obs.t_planning < newobs.t_min:
                    # need to look at the next one this is not a match
                    obscount += 1
                    break
                # anything else is mark it not observed
                # the lists are sorted so if the currect observation is alread
                # newer than this one its newer than te rest
                if notfound or obs.t_planning > newobs.t_max:
                    tomark.append(obs.t_planning)
                notfound = False
        await self.delete_obs(todelete)
        await self.mark_obs(tomark)
        return len(tomark or [])

    async def find_oldest_plan(
        self, status: str = "Scheduled", negate: bool = False
    ) -> float:
        """Look for entries with t_planning  in the past and it is still
        status(default scheduled may never be anything else),
        do the opposite query if negate is true
        the oldes one will give the start time to seach for
        expoosures end time can be now

        """
        comp = "="
        if negate:
            comp = "<>"
        session = AsyncSession(self.engine)
        statement = (
            f"select t_planning as t from "
            f'{self.schema}"{Obsplan.__tablename__}"'
            f" where t_planning > 0 and execution_status {comp} '{status}' "
            f" order by t_planning DESC limit 1 "
        )
        log.debug(statement)
        res = await session.execute(text(statement))
        val = res.fetchone()
        await session.close()
        if val and val[0]:
            return val[0]
        else:
            return 0

    async def mark_old_obs(self) -> None:
        """Mark old observations `Not observed`
        if t_planning is in the past and it is still scheduled
        it is not happening.
        at least if its from yesterday it should go"""
        session = AsyncSession(self.engine)
        t: Time = Time.now() + TimeDelta(30 * u.h)

        told = t.to_value("mjd")
        nob = "Not Observed"
        sched = "Scheduled"
        stmt = (
            f'update {self.schema}"{Obsplan.__tablename__}"'
            f" set execution_status = '{nob}' "
            f" where t_planning < {told} AND execution_status = '{sched}' "
        )
        log.debug(f"mark_old_obs: {stmt}")
        await session.execute(text(stmt))
        await session.commit()
        await session.close()

    async def mark_obs(self, ts: list[float]) -> None:
        """Not observed"""
        if not ts or len(ts) == 0:
            return
        session = AsyncSession(self.engine)
        stmt = (
            f'update {self.schema}"{Obsplan.__tablename__}"'
            f" set execution_status = 'Not Observed' "
            f" where t_planning in ({','.join(str(t) for t in ts)})"
        )
        log.debug(f"mark_obs: {stmt}")
        await session.execute(text(stmt))
        await session.commit()
        await session.close()

    async def delete_obs(self, ts: list[float]) -> None:
        if not ts or len(ts) == 0:
            return
        session = AsyncSession(self.engine)
        stmt = (
            f'delete from {self.schema}"{Obsplan.__tablename__}"'
            f" where t_planning in ({','.join(str(t) for t in ts)})"
        )
        log.debug(stmt)
        await session.execute(text(stmt))
        await session.commit()
        await session.close()

    async def tidyup(self, t: float) -> None:
        session = AsyncSession(self.engine)
        stmt = (
            f'delete from {self.schema}"{Obsplan.__tablename__}"'
            f" where t_planning = {t}"
        )
        log.debug(stmt)
        await session.execute(text(stmt))
        await session.commit()
        await session.close()


class MockDbHelp(DbHelp):
    obslist = list[Obsplan]()

    async def get_schedule(
        self, time: float = 0, start: Time | None = None
    ) -> list[Obsplan]:
        log.warning(f"Using MOCKDBHelp start {start}, time {time} ignored")
        observations = []
        obs = Obsplan()
        obs.t_planning = 60032.194918981484
        obs.s_ra = 90.90909091666666
        obs.s_dec = -74.60384434722222
        obs.rubin_rot_sky_pos = 18.33895879413964
        obs.rubin_nexp = 3
        observations.append(obs)
        return observations

    async def insert_obsplan(self, observations: list[Obsplan]) -> int:
        MockDbHelp.obslist.extend(observations)
        return len(observations or [])


# sort of singleton
dbHelper: DbHelp | None = None


class DbHelpProvider:

    @staticmethod
    def clear() -> None:
        os.environ["database_url"] = ""
        global dbHelper
        dbHelper = None

    @staticmethod
    async def getHelper() -> DbHelp:
        """
        :return: DbHelp the helper
        """
        global dbHelper
        if dbHelper is None:
            if (
                "database_url" in os.environ
                and os.environ["database_url"] != ""
            ):
                config = Configuration()
                driver = "postgresql+asyncpg"
                full_url = (
                    f"{driver}://{config.database_user}:"  # noqa: E231
                    f"{config.database_password}@"
                    f"{config.database_url}/{config.database}"
                )
                if "memory" in config.database_url:
                    driver = "sqlite+aiosqlite"
                    full_url = (
                        f"{driver}:///file:obloctabdb"  # noqa: E231
                        "?mode=memory&cache=shared&uri=true"  # noqa: E231
                    )
                    log.info(f"Creating SQlAlchemy engine with " f"{full_url}")
                else:
                    log.info(
                        f"Creating SQlAlchemy engine with "
                        f"{config.database_user}@{config.database_url}"
                        f"/config.database"
                        f" and schema: {config.database_schema}."
                    )
                engine = create_async_engine(full_url)
                dbHelper = DbHelp(engine=engine)
                dbHelper.schema = config.database_schema
                if len(dbHelper.schema) > 0 and not dbHelper.schema.endswith(
                    "."
                ):
                    dbHelper.schema = f"{dbHelper.schema}."
                log.info(f"Got engine schema='{dbHelper.schema}'")
            else:
                dbHelper = MockDbHelp(None)
                log.warning("Using MOCK DB - database_url  env not set.")

        return dbHelper
