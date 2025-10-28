import os
import pickle

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import text

from obsloctap.consdbhelp import (
    EXPOSURE_FIELDS,
    ConsDbHelp,
    ConsDbHelpProvider,
)
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
    can_see_sky INTEGER DEFAULT 1,
    sky_rotation REAL default 0,
    observation_reason TEXT
);
"""

columns_types = {
    "exposure_id": int,
    "obs_start_mjd": float,
    "obs_end_mjd": float,
    "band": str,
    "physical_filter": str,
    "s_ra": float,
    "s_dec": float,
    "target_name": str,
    "science_program": str,
    "scheduler_note": str,
    "can_see_sky": int,
    "sky_rotation": float,
    "observation_reason": str,
}


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
        with open("tests/consdb60858.pkl", "rb") as f:
            exposures = pickle.load(f)

        # Generate columns and named parameters
        columns = ", ".join(EXPOSURE_FIELDS)
        placeholders = ", ".join(
            f":{field}" for field in EXPOSURE_FIELDS  # noqa: E231
        )

        insert_sql = text(
            f"""
            INSERT INTO exposure ({columns})
                VALUES ({placeholders})
            """
        )
        print(insert_sql)
        # Insert each exposure into the database

        for exp in exposures:
            exp_dict = {}
            c = 0
            assert len(exp) == len(columns_types)
            for col, typ in columns_types.items():
                val = exp[c]
                if val is not None:
                    exp_dict[col] = typ(val)
                else:
                    exp_dict[col] = None
                c += 1
            await conn.execute(insert_sql, exp_dict)
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
