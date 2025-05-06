"""Helper for the efd -so it may be mocked in test."""

__all__ = ["DbHelp", "DbHelpProvider", "OBSPLAN_FIELDS"]

import os
from io import StringIO
from typing import Any, Sequence

import astropy.time
import structlog
from pandas import Timedelta, Timestamp
from sgp4.propagation import false
from sqlalchemy import Row, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    create_async_engine,
)

from obsloctap.models import Obsplan

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
min30 = astropy.time.TimeDelta("30min")


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

    async def get_schedule(self, time: float = 0) -> list[Obsplan]:
        """Return the latest schedule item from the DB.
        We should consider how much that is.. 24 hours worth?
        if time is zero we just take top obsplanLimit rows."""

        config = Configuration()

        whereclause = ""
        if time != 0:
            window = Timestamp.now() + Timedelta(hours=time)
            whereclause = f" where t_planning > {window.to_julian_date()}"

        statement = (
            f"select {self.insert_fields} from "
            f'{self.schema}."{Obsplan.__tablename__}"'
            f" {whereclause}"
            f" order by t_planning limit  {config.obsplanLimit}"
        )
        log.debug(statement)
        session = AsyncSession(self.engine)
        result = await session.execute(text(statement))
        obs = result.all()
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
            f'insert into {self.schema}."{Obsplan.__tablename__}" '
            f"values ({value_str.getvalue()})"
        )
        log.debug(stmt)
        result = await session.execute(text(stmt))
        log.info(f"Inserted 1 Observation. {result}")

    async def insert_obsplan(self, observations: list[Obsplan]) -> int:
        """Insert observations into the DB -
        return the count of inserted rows."""
        session = AsyncSession(self.engine)
        for observation in observations:
            await self.insert_obs(session, observation)
        await session.commit()
        await session.close()
        log.info(f"Inserted and commited {len(observations)} Observations.")
        return len(observations)

    async def remove_flag(
        self, observations: list[Obsplan], priority: int = 2
    ) -> int:
        """Look at the obsplan table wrt to the new schedule,
        if there is a new simlr entry remove the existing one,
        if there is a time with a different entry mark it as not observed.
        Perhaps one could also delete the not observed ones.
        Observations should be sorted on t_planning.

        Returns number of entries marked not executed"""

        # oservations are sorted
        maxt = observations[-1].t_planning
        mint = observations[0].t_planning

        statement = (
            f"select {self.insert_fields} from "
            f'{self.schema}."{Obsplan.__tablename__}"'
            f" where t_planning between {mint} and {maxt}"
            f" order by t_planning"
        )
        log.debug(statement)
        session = AsyncSession(self.engine)
        result = await session.execute(text(statement))
        oldobs: list[Obsplan] = self.process(result.all())
        todelete = list()
        tomark = list()
        obscount = 0
        # loop over the old observations and match to new ones
        for obs in oldobs:
            notfound = True
            while notfound:
                newobs: Obsplan = observations[obscount]
                if obs.t_planning > newobs.t_max:
                    # need to look at the next one
                    obscount = obscount + 1
                    break
                if newobs.t_min < obs.t_planning < newobs.t_max:
                    # same time frame - check other things
                    # - see dmtn-263 Sc 3.2
                    if obs.em_min == newobs.em_min:
                        # its a match so we replace it
                        todelete.append(obs.t_planning)
                        notfound = false
                # anything else is mar it not observed
                if notfound:
                    tomark.append(obs.t_planning)
                notfound = false
        log.info(f"delete {todelete} \n Mark {tomark}")
        await self.delete_obs(todelete)
        await self.mark_obs(tomark)
        return len(tomark)

    async def mark_old_obs(self) -> None:
        """Mark old observations `Not observed`
        if t_planning is in the past and it did
        not happen  it is not happening.
        Now if possibly too agressive but 30 minutes shoudl be ok"""
        session = AsyncSession(self.engine)
        t: Timestamp = Timestamp.now() + Timedelta(minutes=30)
        told = t.to_julian_date()
        nob = "'Not Observed'"
        stmt = (
            f'update {self.schema}."{Obsplan.__tablename__}"'
            f" set execution_status = {nob} "
            f" where t_planning < {told}"
        )
        log.debug(stmt)
        await session.execute(text(stmt))
        await session.commit()
        await session.close()

    async def mark_obs(self, ts: list[float]) -> None:
        """Not observed"""
        if not ts or len(ts) == 0:
            return
        session = AsyncSession(self.engine)
        stmt = (
            f'update {self.schema}."{Obsplan.__tablename__}"'
            f' set execution_status = "Not Observed" '
            f" where t_planning in({ts})"
        )
        log.debug(stmt)
        await session.execute(text(stmt))
        await session.commit()
        await session.close()

    async def delete_obs(self, ts: list[float]) -> None:
        if not ts or len(ts) == 0:
            return
        session = AsyncSession(self.engine)
        stmt = (
            f'delete from {self.schema}."{Obsplan.__tablename__}"'
            f" where t_planning in ({ts})"
        )
        log.debug(stmt)
        await session.execute(text(stmt))
        await session.commit()
        await session.close()

    async def tidyup(self, t: float) -> None:
        session = AsyncSession(self.engine)
        stmt = (
            f'delete from {self.schema}."{Obsplan.__tablename__}"'
            f" where t_planning = {t}"
        )
        log.debug(stmt)
        await session.execute(text(stmt))
        await session.commit()
        await session.close()


class MockDbHelp(DbHelp):
    obslist = list[Obsplan]()

    async def get_schedule(self, time: float = 0) -> list[Obsplan]:
        log.warning("Using MOCKDBHelp")
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
        return len(observations)


# sort of singleton
dbHelper: DbHelp | None = None


class DbHelpProvider:
    @staticmethod
    async def getHelper() -> DbHelp:
        """
        :return: EfdHelp the helper
        """
        global dbHelper
        if dbHelper is None:
            if "database_url" in os.environ:
                config = Configuration()
                full_url = (
                    f"postgresql+asyncpg"
                    f"://{config.database_user}:"  # noqa: E231
                    f"{config.database_password}@"
                    f"{config.database_url}/{config.database}"
                )
                log.info(
                    f"Creating SQlAlchemy engine with "
                    f"{config.database_user}@{config.database_url}"
                    f"/config.database"
                    f" and schema: {config.database_schema}."
                )
                engine = create_async_engine(full_url)
                dbHelper = DbHelp(engine=engine)
                dbHelper.schema = config.database_schema
                log.info("Got engine")
            else:
                dbHelper = MockDbHelp(None)
                log.warning("Using MOCK DB - database_url  env not set.")

        return dbHelper
