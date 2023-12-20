"""Helper for the efd -so it may be mocked in test."""

__all__ = ["DbHelp", "DbHelpProvider"]

import logging
import os
from io import StringIO
from typing import Any, Sequence

from pandas import Timedelta, Timestamp
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
        Process the result of the query.

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
            window = Timestamp.now() - Timedelta(hours=12)
            whereclause = f" where t_planning > {window.to_julian_date()}"

        statement = (
            f"select {self.insert_fields} from "
            f'{self.schema}."{Obsplan.__tablename__}"'
            f" {whereclause}"
            f" order by t_planning limit  {config.obsplanLimit}"
        )
        logging.debug(statement)
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
                value_str.write(f"'{getattr(observation,key)}'")
            if (count + 1) < len(OBSPLAN_FIELDS):
                value_str.write(",")

        stmt = (
            f'insert into {self.schema}."{Obsplan.__tablename__}" '
            f"values ({value_str.getvalue()})"
        )
        logging.debug(stmt)
        result = await session.execute(text(stmt))
        logging.info(f"Inserted 1 Observation. {result}")

    async def insert_obsplan(self, observations: list[Obsplan]) -> int:
        """Insert observations into the DB -
        return the count of inserted rows."""
        session = AsyncSession(self.engine)
        for observation in observations:
            await self.insert_obs(session, observation)
        await session.commit()
        await session.close()
        logging.info(
            f"Inserted and commited {len(observations)} Observations."
        )
        return len(observations)

    async def tidyup(self, t: float) -> None:
        session = AsyncSession(self.engine)
        stmt = (
            f'delete from {self.schema}."{Obsplan.__tablename__}"'
            f" where t_planning = {t}"
        )
        logging.debug(stmt)
        await session.execute(text(stmt))
        await session.commit()
        await session.close()


class MockDbHelp(DbHelp):
    obslist = list[Obsplan]()

    async def get_schedule(self, time: float = 0) -> list[Obsplan]:
        observations = []
        obs = Obsplan()
        obs.t_planning = 60032.194918981484
        obs.s_ra = 90.90909091666666
        obs.s_dec = -74.60384434722222
        obs.rubin_rot_sky_pos = 18.33895879413964
        obs.rubin_nexpnexp = 3
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
                logging.info(
                    f"Creating SQlAlchemy engine with "
                    f"{config.database_url[0:25]}......"
                    f" and schema: {config.database_schema}."
                )
                engine = create_async_engine(config.database_url)
                dbHelper = DbHelp(engine=engine)
                dbHelper.schema = config.database_schema
                logging.info("Got engine")
            else:
                dbHelper = MockDbHelp(None)
                logging.warning("Using MOCK DB - database_url  env not set.")

        return dbHelper
