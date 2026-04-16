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
min20 = TimeDelta("20min")
min15 = TimeDelta("15min")
sec30 = TimeDelta("30s")


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
        self, observation: Obsplan, session: AsyncSession
    ) -> bool:
        """Insert an observation into the DB.
        Returns True if insert was successful, False otherwise."""

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
        try:
            res = await session.execute(text(stmt))
            return res.rowcount > 0
        except Exception as e:
            log.error(f"Failed to insert observation: {e}")
            return False

    async def insert_obsplan(self, observations: list[Obsplan]) -> int:
        """Insert observations into the DB -
        return the count of inserted rows.
        This version avoids creating duplicate entries by checking for an
        existing row with the same t_planning before inserting.
        """
        session = AsyncSession(self.engine)
        inserted = 0
        skipped = 0
        for observation in observations:
            # check for existing entry with same t_planning
            check_stmt = (
                f'select 1 from {self.schema}"{Obsplan.__tablename__}" '
                f"where ABS(t_planning - {observation.t_planning}) < 1e-6 "
                f"limit 1"
            )
            result = await session.execute(text(check_stmt))
            exists = result.fetchone()
            if exists:
                skipped += 1
                continue
            await self.insert_obs(observation, session)
            inserted += 1
        if inserted > 0:
            await session.commit()
        await session.close()
        log.info(
            f"Inserted and commited {inserted} Observations"
            f", Skipped {skipped}."
        )
        return inserted

    async def find_by_obs_id(
        self,
        obs_id: str,
        session: AsyncSession | None = None,
    ) -> Sequence[Row[tuple[Any]]]:
        """Look for matching plan entires based on obs_id"""
        close_session = False
        if session is None:
            session = AsyncSession(self.engine)
            close_session = True
        stmt = (
            f'SELECT * FROM {self.schema}"{Obsplan.__tablename__}" '
            f'WHERE obs_id == "{obs_id}"'
        )
        result = await session.execute(text(stmt))
        matches = result.fetchall()
        if close_session:
            await session.close()
        return matches

    async def find_entries(
        self,
        ra: float,
        dec: float,
        obs_start_mjd: float,
        session: AsyncSession,
        tol: float = 3.0,
        timetol: TimeDelta = min20,
    ) -> Sequence[Row[tuple[Any]]]:
        """Look for matching plan entires based on position and rough time"""

        # Convert TimeDelta to MJD value (in days)
        timetol_mjd = timetol.to_value("jd")

        stmt = (
            f'SELECT * FROM {self.schema}"{Obsplan.__tablename__}" '
            f"WHERE ABS(s_ra - {ra}) < {tol} "
            f"AND ABS(s_dec - {dec}) < {tol} "
            f"AND t_min <= {obs_start_mjd + timetol_mjd} AND "
            f"t_max >= {obs_start_mjd - timetol_mjd}"
        )
        result = await session.execute(text(stmt))
        matches = result.fetchall()
        return matches

    async def update_insert_exposures(
        self,
        exposures: list[Exposure],
        tol: float = 0.01,
        timetol: TimeDelta = sec30,
    ) -> tuple[int, int]:
        """
        Update obsplan entries that match exposures by s_ra, s_dec,
        and obs_start_mjd in [t_min, t_max].
        tol: tolerances for matching s_ra and s_dec.
        Check it is not 'performed' if it is onlyupdate if same id.
        Returns the number of updated and inserted entries in a tupple.
        """
        session = AsyncSession(self.engine)
        updated = 0
        unmatched = 0
        inserted = 0
        for exp in exposures:
            # Find matching obsplan entries
            done = False
            # simple case obs_id matches exposure_id
            idmatch = await self.find_by_obs_id(exp.exposure_id, session)
            if len(idmatch) > 0:
                # update execution_status to 'Performed' etc
                if await self.update_one(exp, idmatch[0], session):
                    updated += 1
                    done = True
                else:
                    log.error(
                        f"Failed to update exp: {exp.exposure_id} - "
                        f"obs: {idmatch[0].obs_id} "
                    )

            if not done:
                matches = await self.find_entries(
                    exp.s_ra,
                    exp.s_dec,
                    exp.obs_start_mjd,
                    session,
                    tol,
                    timetol,
                )
                for match in matches:
                    if match.execution_status != "Performed":
                        # Found a match that is not yet performed
                        if await self.update_one(exp, match, session):
                            updated += 1
                            done = True
                        else:
                            log.error(
                                f"Failed to update exp: {exp.exposure_id} - "
                                f"obs: {match.obs_id} "
                            )
                        break

            if not done:
                unmatched += 1
                if await self.insert_exposure(exp, session):
                    inserted += 1

        await session.commit()
        await session.close()
        log.info(
            f"Updated {updated}, unmatched {unmatched}, "
            f"inserted {inserted} of {len(exposures)}"
        )
        return (updated, inserted)

    async def insert_exposure(
        self, exp: Exposure, session: AsyncSession
    ) -> bool:
        """Put in an obsplan line based on an exposure.
        This is when consdb has an observation but it does not match
        any planned item.
        Returns True if insert was successful, False otherwise."""
        if not exp.band or exp.band not in spectral_ranges:
            log.warning(
                f"{exp.exposure_id} has no band - "
                f"will insert 'other:pinhole'"
            )
            exp.band = "other:pinhole"
        value_str = (
            f"0, "  # t_planning
            f"'{exp.target_name}', "  # target_name
            f"'{str(exp.exposure_id)}', "  # obs_id
            f"'{exp.observation_reason}-{exp.scheduler_note}', "
            # obs_collection
            f"{exp.s_ra}, "  # s_ra
            f"{exp.s_dec}, "  # s_dec
            f"3, "  # s_fov
            f"'', "  # s_region
            f"0.2, "  # s_resolution
            f"{exp.obs_start_mjd}, "  # t_min
            f"{exp.obs_end_mjd}, "  # t_max
            f"{exp.obs_end_mjd - exp.obs_start_mjd}, "  # t_exptime
            f"15, "  # t_resolution
            f"'{spectral_ranges[
                exp.band or 'other:pinhole'][0]}', "
            f"'{spectral_ranges[
                exp.band or 'other:pinhole'][1]}', "
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
        try:
            res = await session.execute(text(insert_stmt))
            return res.rowcount > 0
        except Exception as e:
            log.error(f"Failed to insert exposure {exp.exposure_id}: {e}")
            return False

    async def update_planif(self, a: Obsplan, b: Obsplan) -> Obsplan:
        """
        if a has some values update them in B.
        typically for nextVisit
        Parameters
        ----------
        a
        b

        Returns
        -------
        b updated with values from A.
        """

        fields = [
            "t_planning",  # DOUBLE PRECISION NOT NULL,
            "target_name",  # VARCHAR,
            "obs_id",  # VARCHAR NOT NULL,
            "obs_collection",  # VARCHAR,
            "s_ra",  # DOUBLE PRECISION,
            "s_dec",  # DOUBLE PRECISION,
            "t_min",  # DOUBLE PRECISION NOT NULL,
            "t_max",  # DOUBLE PRECISION NOT NULL,
            "em_min",  # DOUBLE PRECISION,
            "em_max",  # DOUBLE PRECISION,
            "priority",  # INTEGER NOT NULL,
            "rubin_rot_sky_pos",  # FLOAT,
            "rubin_nexp",  # INTEGER
        ]
        noval = [0, "", "", "", 0, 0, 0, 0, 0, 0, 0, 0, 0]

        for c, key in enumerate(fields):
            new = getattr(a, key)
            if new != noval[c]:
                setattr(b, key, new)
        return b

    async def update_insert_nextVisit(self, obs: Obsplan) -> int:
        """
        For given observation find it and update it or insert it
        Parameters
        ----------
        obs

        Returns
        -------
        0 for update and 1 for insert
        """

        session = AsyncSession(self.engine)
        # next visit can be any time but 20min seems plenty (PP use 15min)
        matches = await self.find_entries(
            obs.s_ra, obs.s_dec, obs.t_planning, session, 1.0, min20
        )
        log.debug(f"update_insert_nextVisit: got  {len(matches)} matches")
        retval = 0
        if len(matches) > 0:
            plan = self.process(matches)[0]
            t_planning = plan.t_planning
            await self.update_planif(plan, obs)
            #  we keep t_planning from the exisiting enty -
            #  nextVisit has no time.
            plan.t_planning = t_planning
            await self.update_obsplan(plan, session)
        else:
            if await self.insert_obs(obs, session):
                retval = 1
        await session.commit()
        await session.close()
        return retval

    async def update_obsplan(
        self, obs: Obsplan, session: AsyncSession
    ) -> None:
        """update an obsplan line used for nextVisit"""
        update_stmt = (
            f'UPDATE {self.schema}"{Obsplan.__tablename__}" '
            f"SET "
            f"target_name = '{obs.target_name}', "
            f"obs_collection = '{obs.obs_collection}', "
            f"em_min = {obs.em_min}, "
            f"em_max = {obs.em_max}, "
            f"t_min = {obs.t_min}, "
            f"t_max = {obs.t_max}, "
            f"priority = {obs.priority}, "
            f"rubin_rot_sky_pos = {obs.rubin_rot_sky_pos} "
            f"WHERE obs_id = '{obs.obs_id}'"
        )
        await session.execute(text(update_stmt))

    async def update_one(
        self, exp: Exposure, match: Row, session: AsyncSession
    ) -> bool:
        """update an obsplan line based on an exposure
         setting it to performed
        this was using obsid - but is that in obsplan?."""
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
        res = await session.execute(text(update_stmt))
        return res.rowcount > 0

    async def remove_old(self, observations: list[Obsplan]) -> int:
        """Delete scheduled obsplan rows in the overall time window
        covered by the passed observations.

        Observations should be sorted by time so that:
        - observations[0].t_min is the earliest start
        - observations[-1].t_max is the latest end

        Returns number of rows deleted.
        """
        if not observations:
            return 0

        maxt = observations[0].t_min
        mint = observations[-1].t_max
        sched = "Scheduled"
        abort = "Aborted"

        session = AsyncSession(self.engine)
        stmt = (
            f'delete from {self.schema}"{Obsplan.__tablename__}" '
            f"where t_planning between {mint} and {maxt} "
            f"and execution_status in ('{sched}', '{abort}') "
            f""
        )
        log.debug(f"remove_old: {stmt}")
        res = await session.execute(text(stmt))
        await session.commit()
        await session.close()
        return res.rowcount or 0

    async def remove_flag(
        self, observations: list[Obsplan], priority: int = 2
    ) -> int:
        """Look at the obsplan table wrt to the new schedule,
        delete observations in the time window of this new plan
        if they are "'cheduled'
        If there is an existing observation that falls within the time
        window of a new scheduled observation and it has a different
        obs_id, delete the old one (it will be replaced by the new).
        Any old observations within the overall window that are not
        matched to a new observation will be marked as Not Observed.

        Observations should be sorted on t_planning (descending).

        Returns number of entries marked not executed"""

        if len(observations) == 0:
            return 0
        # Observations are expected to be sorted descending
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

        # Build a set of old t_planning values that get deleted/replaced
        todelete: list[float] = []
        tomark: list[float] = []

        # For faster matching, create index by t_planning for new observations
        new_by_window = []
        for newobs in observations:
            new_by_window.append((newobs.t_min, newobs.t_max, newobs.obs_id))

        # For each old observation, see if it falls within any new obs window
        for old in oldobs:
            matched = False
            for tmin, tmax, new_id in new_by_window:
                if tmin <= old.t_planning <= tmax:
                    matched = True
                    # if the ids differ, the old should be deleted (replaced)
                    if old.obs_id != new_id:
                        todelete.append(old.t_planning)
                    # if ids are same, keep the old (it matches)
                    break
            if not matched:
                # not matched to any new window -> mark as Not Observed
                tomark.append(old.t_planning)

        # apply deletes and marks
        await self.delete_obs(todelete)
        await self.mark_not_observed(tomark)
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
        """Mark old observations `Aborted`
        if t_planning is in the past and it is still scheduled
        it is not happening.
        at least if its from yesterday it should go"""
        session = AsyncSession(self.engine)
        t: Time = Time.now() + TimeDelta(30 * u.h)

        told = t.to_value("mjd")
        nob = "Aborted"
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

    async def mark_not_observed(self, ts: list[float]) -> int:
        """Mark observations as Not Observed.
        Returns number of rows updated."""
        if not ts or len(ts) == 0:
            return 0
        session = AsyncSession(self.engine)
        stmt = (
            f'update {self.schema}"{Obsplan.__tablename__}"'
            f" set execution_status = 'Not Observed' "
            f" where t_planning in ({','.join(str(t) for t in ts)})"
        )
        log.debug(f"mark_obs: {stmt}")
        res = await session.execute(text(stmt))
        await session.commit()
        await session.close()
        return res.rowcount

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
        obs.em_min = spectral_ranges["r"][0]
        obs.em_max = spectral_ranges["r"][1]
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
                    f"{driver}://{config.database_user}:"
                    f"{config.database_password}@"
                    f"{config.database_url}/{config.database}"
                )
                if "memory" in config.database_url:
                    driver = "sqlite+aiosqlite"
                    full_url = (
                        f"{driver}:///file:obloctabdb"
                        "?mode=memory&cache=shared&uri=true"
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
