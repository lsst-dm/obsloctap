"""Helper for the efd -so it may be mocked in test."""

__all__ = ["DbHelp", "DbHelpProvider"]

import logging
import os
from typing import Any

from pandas import Timedelta, Timestamp
from safir.database import create_database_engine
from sqlalchemy import ScalarResult, Select, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from obsloctap.models import Observation, Obsplan

from .config import Configuration

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

    def process(self, result: ScalarResult[Any]) -> list[Observation]:
        """
        Process the result of the query.

        :type self: DnHelp
        :type result: Obsplan[]
        :return: list[Observation]
        """
        obslist = list[Observation]()
        for o in result:
            mjd = o.t_planning
            ra = o.s_ra
            dec = o.s_dec
            rotSkyPos = o.rot_sky_pos
            nexp = o.nexp
            obs = Observation(
                mjd=mjd, ra=ra, dec=dec, rotSkyPos=rotSkyPos, nexp=nexp
            )
            obslist.append(obs)
        return obslist

    async def get_schedule(self) -> list[Observation]:
        """Return the latest schedule item from the DB.
        We should consider how much that is.. 24 hours worth?"""

        time = Timestamp.now() - Timedelta(hours=12)
        statement: Select = select(Obsplan).where(Obsplan.t_planning > time)
        session = AsyncSession(self.engine)
        session.add(statement)
        result = await session.execute(statement)
        obs = result.scalars()
        await session.close()
        return self.process(obs)

    async def insert_obsplan(self, observations: list[Observation]) -> int:
        """Insert observations into the DB -
        return the count of inserted rows."""
        session = AsyncSession(self.engine)
        for observation in observations:
            session.add(observation)
        await session.commit()
        await session.close()
        return len(observations)


class MockDbHelp(DbHelp):
    obslist = list[Observation]()

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

    async def insert_obsplam(self, observations: list[Observation]) -> int:
        MockDbHelp.obslist.extend(observations)
        return len(observations)


# sort of singleton
dbHelper: DbHelp | None = None


class DbHelpProvider:
    @staticmethod
    def getHelper() -> DbHelp:
        """
        :return: EfdHelp the helper
        """
        global dbHelper
        if dbHelper is None:
            if "database_url" in os.environ:
                config = Configuration()
                engine = create_database_engine(
                    config.database_url,
                    config.database_password,
                    schema=config.database_schema,
                )
                dbHelper = DbHelp(engine=engine)
            else:
                dbHelper = MockDbHelp(None)
                logging.warning("Using MOCK DB - database_url  env not set.")

        return dbHelper
