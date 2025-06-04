import os

from sqlalchemy.sql import text

from obsloctap.db import DbHelp, DbHelpProvider

__all__ = ["SqliteDbHelp"]


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

    @staticmethod
    async def getSqlLite() -> DbHelp:
        os.environ["datase_url"] = ":memory:"
        db = await DbHelpProvider.getHelper()
        return db
