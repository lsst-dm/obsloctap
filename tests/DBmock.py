import os
import pickle

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import text

from obsloctap.consdb import ConsDbHelp, ConsDbHelpProvider
from obsloctap.db import DbHelp, DbHelpProvider

__all__ = ["SqliteDbHelp"]


EXPOSURE_DDL = """
CREATE TABLE exposure (
    exposure_id INTEGER PRIMARY KEY,
    obs_start_mjd REAL NOT NULL,
    obs_end_mjd REAL NOT NULL,
    band TEXT,
    physical_filter TEXT,
    s_ra REAL,
    s_dec REAL,
    target_name TEXT,
    science_program TEXT,
    scheduler_note TEXT,
    can_see_sky INTEGER DEFAULT 1
);
"""


class SqliteDbHelp:
    async def setup_schema(self) -> None:
        db = await SqliteDbHelp.getSqlLite()
        with open("obsplan.sql", "r") as schema:
            conn = db.get_session()
            ddl = schema.read().split(";")
            try:  # make sure we are empty
                await conn.execute(text('drop table "ObsPlan";'))  # noqa: E702
                await conn.commit()
            except Exception as e:
                print(e)
            try:
                await conn.execute(text(f"{ddl[0]}; "))  # noqa: E702
                await conn.commit()
            except Exception as e:
                print(e)
            finally:
                await conn.close()

    async def setup_consdb_schema(self) -> None:
        db = await SqliteDbHelp.getSqlLite()
        conn = db.get_session()
        try:
            await conn.execute(text("DROP TABLE IF EXISTS exposure;"))
            await conn.commit()
        except Exception as e:
            print(e)
        try:
            await conn.execute(text(EXPOSURE_DDL))
            await conn.commit()
        except Exception as e:
            print(e)
        try:
            await self.load_and_insert_consdb(conn)
        except Exception as e:
            print(e)
        finally:
            await conn.close()

    async def load_and_insert_consdb(self, conn: AsyncSession) -> None:
        # Load data from pickle file for testing
        with open("tests/consdb.pkl", "rb") as f:
            exposures = pickle.load(f)

        # Insert each exposure into the database
        insert_sql = text(
            """
            INSERT INTO exposure (
                exposure_id, obs_start_mjd, obs_end_mjd, band,
                physical_filter, s_ra, s_dec,
                target_name, science_program, scheduler_note
            ) VALUES (
                :exposure_id, :obs_start_mjd, :obs_end_mjd, band:,
                :physical_filter, :s_ra, :s_dec,
                :target_name, :science_program, :scheduler_note
            )
        """
        )

        for exp in exposures:
            print(f"{exp['expousure_id']}")
            res = await conn.execute(insert_sql, exp)
            print(res)
        await conn.commit()

    @staticmethod
    async def getSqlLite() -> DbHelp:
        os.environ["datase_url"] = ":memory:"
        db = await DbHelpProvider.getHelper()
        return db

    @staticmethod
    async def getConsDbMock() -> ConsDbHelp:
        os.environ["datase_url"] = ":memory:"
        helper = await ConsDbHelpProvider.getHelper()
        return helper
